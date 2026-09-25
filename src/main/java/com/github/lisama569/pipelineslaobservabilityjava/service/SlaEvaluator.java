package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * SLA 评估器：对同一组输入事件产生可复现的评估结论。
 *
 * <p>评估完全基于事件时间水位线（最大 occurredAt），不读取墙钟，
 * 因此同样的事件集合无论到达顺序如何都会得到相同结论。
 *
 * <p>指标定义：
 * <ul>
 *   <li>新鲜度：水位线 - 最近一次 SUCCESS 完成时间，超过 maxFreshnessLag 违约；
 *       窗口内有事件但从未成功完成也视为违约。</li>
 *   <li>调度延迟：窗口内 STARTED 事件的 occurredAt - scheduledAt 的最大值，
 *       超过 maxScheduleDelay 违约。</li>
 *   <li>SLA 达标率：窗口内 FINISHED 运行中 SUCCESS 的占比，
 *       低于 minSlaAttainment 违约。</li>
 * </ul>
 */
@Service
public class SlaEvaluator {

    private final EventStore eventStore;
    private final SlaProperties properties;

    public SlaEvaluator(EventStore eventStore, SlaProperties properties) {
        this.eventStore = eventStore;
        this.properties = properties;
    }

    /** 评估单条流水线当前状态（基于事件时间水位线）。 */
    public PipelineAssessment evaluate(String pipelineId) {
        SlaProperties.PipelineRule rule = properties.ruleFor(pipelineId);
        Optional<Instant> wm = eventStore.watermark();
        if (wm.isEmpty()) {
            return new PipelineAssessment(pipelineId, null, null, false, null, 0L, false, null, false, List.of());
        }
        Instant watermark = wm.get();
        Instant windowStart = watermark.minus(rule.getWindow());

        List<JobRunEvent> windowEvents = eventStore.eventsInWindow(pipelineId, windowStart, watermark);

        // 新鲜度需要回看 maxFreshnessLag，可能超出评估窗口。
        Duration freshnessLookback = rule.getMaxFreshnessLag().compareTo(rule.getWindow()) > 0
                ? rule.getMaxFreshnessLag() : rule.getWindow();
        List<JobRunEvent> freshnessEvents = freshnessLookback.equals(rule.getWindow())
                ? windowEvents
                : eventStore.eventsInWindow(pipelineId, watermark.minus(freshnessLookback), watermark);

        Optional<Instant> lastSuccess = freshnessEvents.stream()
                .filter(e -> e.eventType() == EventType.FINISHED && e.status() == RunStatus.SUCCESS)
                .map(JobRunEvent::occurredAt)
                .max(Comparator.naturalOrder());

        Long freshnessLag = lastSuccess.map(t -> Duration.between(t, watermark).getSeconds()).orElse(null);
        boolean freshnessBreached = lastSuccess.isEmpty()
                ? !windowEvents.isEmpty()
                : freshnessLag > rule.getMaxFreshnessLag().getSeconds();

        long maxDelay = windowEvents.stream()
                .filter(e -> e.eventType() == EventType.STARTED)
                .mapToLong(e -> Math.max(0L, Duration.between(e.scheduledAt(), e.occurredAt()).getSeconds()))
                .max().orElse(0L);
        boolean delayBreached = maxDelay > rule.getMaxScheduleDelay().getSeconds();

        List<JobRunEvent> finished = windowEvents.stream()
                .filter(e -> e.eventType() == EventType.FINISHED)
                .toList();
        Double attainment = null;
        if (!finished.isEmpty()) {
            long ok = finished.stream().filter(e -> e.status() == RunStatus.SUCCESS).count();
            attainment = (double) ok / finished.size();
        }
        boolean slaBreached = attainment != null && attainment < rule.getMinSlaAttainment();

        Set<String> keys = new LinkedHashSet<>();
        freshnessEvents.forEach(e -> keys.add(EventStore.dedupKey(e)));
        windowEvents.forEach(e -> keys.add(EventStore.dedupKey(e)));

        return new PipelineAssessment(pipelineId, watermark, windowStart,
                freshnessBreached, freshnessLag, maxDelay, delayBreached,
                attainment, slaBreached, new ArrayList<>(keys));
    }

    /** 评估所有已知流水线。 */
    public List<PipelineAssessment> evaluateAll() {
        return eventStore.knownPipelines().stream().map(this::evaluate).toList();
    }
}
