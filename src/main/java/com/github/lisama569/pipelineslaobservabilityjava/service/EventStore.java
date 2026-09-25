package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.RejectReason;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件存储：负责上报的校验、幂等去重、乱序归位与有界保留。
 *
 * <p>语义约定：
 * <ul>
 *   <li>去重键 = pipelineId + runId + eventType；同键同内容 = DUPLICATE（无副作用），
 *       同键不同内容 = CONFLICTING_EVENT（拒绝）。</li>
 *   <li>乱序/迟到事件按 occurredAt 归位，评估只依赖事件时间，结论与到达顺序无关。</li>
 *   <li>同一 run 内 STARTED 晚于已接受的 FINISHED（或 FINISHED 早于已接受的 STARTED）
 *       视为时钟回拨矛盾，拒绝并返回 EVENT_TIME_REGRESSION。</li>
 *   <li>早于 (水位线 - retention) 的事件被拒绝（BEYOND_RETENTION），
 *       已存事件超过保留期会被清理，保证存储与评估开销有界。</li>
 * </ul>
 * 所有公共方法加锁，保证并发上报下的线性一致性。
 */
@Service
public class EventStore {

    /** 排序键：(occurredAt, dedupKey)，保证同一时刻多条事件顺序确定。 */
    private record EventOrder(Instant at, String key) implements Comparable<EventOrder> {
        @Override
        public int compareTo(EventOrder other) {
            int c = at.compareTo(other.at);
            return c != 0 ? c : key.compareTo(other.key);
        }
    }

    /** 单条流水线的有界事件集合。 */
    private static final class PipelineData {
        final Map<String, JobRunEvent> byKey = new HashMap<>();
        final Map<String, List<JobRunEvent>> byRun = new HashMap<>();
        final TreeMap<EventOrder, JobRunEvent> sorted = new TreeMap<>();
    }

    private final SlaProperties properties;
    private final Map<String, PipelineData> pipelines = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private Instant watermark;

    public EventStore(SlaProperties properties) {
        this.properties = properties;
    }

    static String dedupKey(JobRunEvent e) {
        return e.pipelineId() + "|" + e.runId() + "|" + e.eventType();
    }

    /** 接收一条上报事件（幂等）。 */
    public IngestResult ingest(JobRunEvent event) {
        synchronized (lock) {
            IngestResult invalid = validate(event);
            if (invalid != null) {
                return invalid;
            }
            String key = dedupKey(event);
            PipelineData data = pipelines.computeIfAbsent(event.pipelineId(), k -> new PipelineData());

            JobRunEvent existing = data.byKey.get(key);
            if (existing != null) {
                if (existing.equals(event)) {
                    return IngestResult.duplicate("同键同内容，幂等忽略: " + key);
                }
                return IngestResult.rejected(RejectReason.CONFLICTING_EVENT,
                        "同键不同内容: " + key + " 既有=" + existing + " 新=" + event);
            }

            IngestResult regression = checkRunOrdering(data, event);
            if (regression != null) {
                return regression;
            }

            Instant horizon = retentionHorizon();
            if (horizon != null && event.occurredAt().isBefore(horizon)) {
                return IngestResult.rejected(RejectReason.BEYOND_RETENTION,
                        "事件时间 " + event.occurredAt() + " 早于保留下界 " + horizon);
            }

            data.byKey.put(key, event);
            data.byRun.computeIfAbsent(event.runId(), k -> new ArrayList<>()).add(event);
            data.sorted.put(new EventOrder(event.occurredAt(), key), event);
            if (watermark == null || event.occurredAt().isAfter(watermark)) {
                watermark = event.occurredAt();
            }
            prune(data);
            return IngestResult.accepted("已接受: " + key);
        }
    }

    private IngestResult validate(JobRunEvent event) {
        if (event == null
                || isBlank(event.pipelineId()) || isBlank(event.runId())
                || event.eventType() == null
                || event.scheduledAt() == null || event.occurredAt() == null) {
            return IngestResult.rejected(RejectReason.INVALID_EVENT, "必填字段缺失: " + event);
        }
        if (event.eventType() == EventType.FINISHED && event.status() == null) {
            return IngestResult.rejected(RejectReason.MISSING_STATUS,
                    "FINISHED 事件缺少终态: " + dedupKey(event));
        }
        if (properties.isRequireRegisteredPipelines()
                && !properties.getPipelines().containsKey(event.pipelineId())) {
            return IngestResult.rejected(RejectReason.UNKNOWN_PIPELINE,
                    "流水线未注册: " + event.pipelineId());
        }
        return null;
    }

    /** 同 run 内事件时间矛盾检测（时钟回拨防护）。 */
    private IngestResult checkRunOrdering(PipelineData data, JobRunEvent event) {
        for (JobRunEvent other : data.byRun.getOrDefault(event.runId(), List.of())) {
            boolean contradiction =
                    (event.eventType() == EventType.STARTED && other.eventType() == EventType.FINISHED
                            && event.occurredAt().isAfter(other.occurredAt()))
                    || (event.eventType() == EventType.FINISHED && other.eventType() == EventType.STARTED
                            && event.occurredAt().isBefore(other.occurredAt()));
            if (contradiction) {
                return IngestResult.rejected(RejectReason.EVENT_TIME_REGRESSION,
                        "与已接受事件矛盾: 新=" + event + " 既有=" + other);
            }
        }
        return null;
    }

    /** 有效保留下界：水位线 - max(retention, 最大评估窗口)。 */
    private Instant retentionHorizon() {
        if (watermark == null) {
            return null;
        }
        var retention = properties.getRetention().compareTo(properties.maxWindow()) > 0
                ? properties.getRetention() : properties.maxWindow();
        return watermark.minus(retention);
    }

    /** 清理保留期之前的事件，保证存储有界。 */
    private void prune(PipelineData data) {
        Instant horizon = retentionHorizon();
        if (horizon == null) {
            return;
        }
        var head = data.sorted.headMap(new EventOrder(horizon, ""), false);
        List<EventOrder> expired = List.copyOf(head.keySet());
        for (EventOrder order : expired) {
            JobRunEvent removed = data.sorted.remove(order);
            data.byKey.remove(order.key());
            if (removed != null) {
                List<JobRunEvent> runEvents = data.byRun.get(removed.runId());
                if (runEvents != null) {
                    runEvents.remove(removed);
                    if (runEvents.isEmpty()) {
                        data.byRun.remove(removed.runId());
                    }
                }
            }
        }
    }

    /** 当前事件时间水位线（所有已接受事件 occurredAt 的最大值）。 */
    public Optional<Instant> watermark() {
        synchronized (lock) {
            return Optional.ofNullable(watermark);
        }
    }

    /** 查询某流水线窗口内的事件（按 occurredAt 升序，键稳定）。 */
    public List<JobRunEvent> eventsInWindow(String pipelineId, Instant windowStart, Instant windowEnd) {
        synchronized (lock) {
            PipelineData data = pipelines.get(pipelineId);
            if (data == null) {
                return List.of();
            }
            return List.copyOf(data.sorted
                    .subMap(new EventOrder(windowStart, ""), true,
                            new EventOrder(windowEnd, Character.toString(Character.MAX_CODE_POINT)), true)
                    .values());
        }
    }

    /** 当前已知（有保留期内事件的）流水线。 */
    public List<String> knownPipelines() {
        synchronized (lock) {
            return pipelines.entrySet().stream()
                    .filter(e -> !e.getValue().sorted.isEmpty())
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
