package com.github.lisama569.pipelineslaobservabilityjava.ingestion;

import com.github.lisama569.pipelineslaobservabilityjava.clock.Clock;
import com.github.lisama569.pipelineslaobservabilityjava.domain.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestOutcome;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.domain.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RunState;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.evaluation.EvaluationService;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 事件上报入口。
 *
 * <p>确定语义：
 * <ul>
 *   <li><b>幂等</b>：eventId 为幂等键；载荷完全一致的重复上报返回 DUPLICATE 与首次接收序号，
 *       不重复计数、不改变既有结论、不重新评估。</li>
 *   <li><b>乱序/迟到</b>：事件先按 eventTime、同刻按服务端单调接收序号 ingestSeq 排序，
 *       与到达顺序无关；eventTime 早于 now-retentionSeconds 才拒绝为 EXPIRED，
 *       保留范围内的迟到事件照常落库并参与下次评估。</li>
 *   <li><b>时钟回拨</b>：接收序号单调递增且不依赖墙上时钟；receivedAt 仅记录不参与排序，
 *       回拨不破坏确定性。eventTime 领先服务端时钟超过 futureToleranceSeconds 拒绝为
 *       FUTURE_EVENT_TIME（上报方时钟快过头通常意味着误报）。</li>
 *   <li><b>矛盾拒绝</b>：同 eventId 载荷不一致、同一 run 重复 STARTED/终态、
 *       终态互相矛盾、起止时间倒挂等均拒绝为 CONFLICT；拒绝前不做任何状态变更（不部分生效）。</li>
 * </ul>
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final PipelineRegistry registry;
    private final Clock clock;
    private final EvaluationService evaluationService;

    public IngestService(PipelineRegistry registry, Clock clock, EvaluationService evaluationService) {
        this.registry = registry;
        this.clock = clock;
        this.evaluationService = evaluationService;
    }

    /** 处理单个事件上报。 */
    public IngestResult report(EventReport report) {
        Instant receivedAt = clock.now();
        if (report == null || isBlank(report.pipelineId())) {
            IngestResult malformed = reject(report == null ? null : report.eventId(),
                    RejectReason.MALFORMED,
                    report == null ? "empty request body" : "pipelineId is required", -1);
            log.info("ingest decision {} | input={} receivedAt={}", malformed, report, receivedAt);
            return malformed;
        }
        PipelineState state = registry.find(report.pipelineId()).orElse(null);
        if (state == null) {
            IngestResult unregistered = reject(report.eventId(), RejectReason.UNREGISTERED_PIPELINE,
                    "pipeline not registered: " + report.pipelineId(), -1);
            log.info("ingest decision {} | input={} receivedAt={}", unregistered, report, receivedAt);
            return unregistered;
        }
        IngestResult result = registry.withLock(report.pipelineId(), s -> doReport(s, report, receivedAt));
        log.info("ingest decision {} | input={} receivedAt={}", result, report, receivedAt);
        return result;
    }

    /** 批量上报：逐条独立校验、独立给出结论，任何一条被拒都不影响其他条目。 */
    public List<IngestResult> reportBatch(List<EventReport> reports) {
        List<IngestResult> results = new ArrayList<>();
        if (reports != null) {
            for (EventReport report : reports) {
                results.add(report(report));
            }
        }
        return results;
    }

    /** 在流水线锁内执行：先校验后变更，保证拒绝不产生部分生效。 */
    private IngestResult doReport(PipelineState state, EventReport r, Instant receivedAt) {
        // ---- 1. 结构校验 MALFORMED ----
        IngestResult malformed = validateShape(r);
        if (malformed != null) {
            return malformed;
        }
        EventType type;
        try {
            type = EventType.valueOf(r.eventType());
        } catch (IllegalArgumentException e) {
            return reject(r.eventId(), RejectReason.MALFORMED,
                    "unknown eventType: " + r.eventType(), -1);
        }

        PipelineConfig config = state.getConfig();
        Instant eventTime = r.eventTime();

        // ---- 2. 时间合理性：未来时间容差 / 迟到保留范围 ----
        Instant futureLimit = receivedAt.plusSeconds(config.futureToleranceSeconds());
        if (eventTime.isAfter(futureLimit)) {
            return reject(r.eventId(), RejectReason.FUTURE_EVENT_TIME,
                    "eventTime " + eventTime + " is more than "
                            + config.futureToleranceSeconds() + "s ahead of receivedAt " + receivedAt, -1);
        }
        Instant expireCutoff = receivedAt.minusSeconds(config.retentionSeconds());
        if (eventTime.isBefore(expireCutoff)) {
            return reject(r.eventId(), RejectReason.EXPIRED,
                    "eventTime " + eventTime + " is older than retention cutoff " + expireCutoff
                            + " (retention=" + config.retentionSeconds() + "s)", -1);
        }

        // ---- 3. 幂等键处理 ----
        JobRunEvent existing = state.getEventsById().get(r.eventId());
        if (existing != null) {
            if (samePayload(existing, r, type)) {
                return new IngestResult(IngestOutcome.DUPLICATE, r.eventId(), null,
                        "identical event already accepted as ingestSeq=" + existing.ingestSeq()
                                + "; no state change", existing.ingestSeq());
            }
            return reject(r.eventId(), RejectReason.CONFLICT,
                    "eventId " + r.eventId() + " already exists with different payload: existing="
                            + existing, -1);
        }

        // ---- 4. 运行生命周期矛盾检测（拒绝发生在任何写入之前）----
        RunState run = state.getRuns().get(r.runId());
        String conflict = lifecycleConflict(run, r, type);
        if (conflict != null) {
            return reject(r.eventId(), RejectReason.CONFLICT, conflict, -1);
        }

        // ---- 5. 全部通过：分配单调序号并落库 ----
        long seq = registry.nextIngestSeq();
        JobRunEvent event = new JobRunEvent(
                r.pipelineId(), r.runId(), r.eventId(), type, eventTime,
                r.scheduledTime(), receivedAt, seq);
        state.getEventsById().put(event.eventId(), event);
        state.getOrderedEvents().put(new PipelineState.EventOrderKey(eventTime, seq), event);

        RunState target = state.getRuns().computeIfAbsent(r.runId(),
                id -> new RunState(r.pipelineId(), id));
        target.getEventIds().add(r.eventId());
        if (target.getFirstIngestSeq() == 0) {
            target.setFirstIngestSeq(seq);
        }
        target.setLastIngestSeq(seq);
        switch (type) {
            case STARTED -> {
                target.setStartedAt(eventTime);
                if (r.scheduledTime() != null) {
                    target.setScheduledTime(r.scheduledTime());
                }
                if (target.getStatus() == null) {
                    target.setStatus(RunStatus.RUNNING);
                }
            }
            case SUCCEEDED -> {
                target.setFinishedAt(eventTime);
                target.setTerminalType(EventType.SUCCEEDED);
                target.setStatus(RunStatus.SUCCEEDED);
            }
            case FAILED -> {
                target.setFinishedAt(eventTime);
                target.setTerminalType(EventType.FAILED);
                target.setStatus(RunStatus.FAILED);
            }
        }

        log.info("event accepted pipeline={} run={} event={} type={} eventTime={} seq={}",
                r.pipelineId(), r.runId(), r.eventId(), type, eventTime, seq);

        // ---- 6. 同锁内立即重估，保证查询可见最新结论 ----
        evaluationService.evaluateWithinLock(state, receivedAt);
        return new IngestResult(IngestOutcome.ACCEPTED, r.eventId(), null,
                "event accepted and pipeline re-evaluated at " + receivedAt, seq);
    }

    private static IngestResult validateShape(EventReport r) {
        if (r == null) {
            return reject(null, RejectReason.MALFORMED, "empty request body", -1);
        }
        if (isBlank(r.pipelineId())) {
            return reject(r.eventId(), RejectReason.MALFORMED, "pipelineId is required", -1);
        }
        if (isBlank(r.runId())) {
            return reject(r.eventId(), RejectReason.MALFORMED, "runId is required", -1);
        }
        if (isBlank(r.eventId())) {
            return reject(r.eventId(), RejectReason.MALFORMED, "eventId is required", -1);
        }
        if (isBlank(r.eventType())) {
            return reject(r.eventId(), RejectReason.MALFORMED, "eventType is required", -1);
        }
        if (r.eventTime() == null) {
            return reject(r.eventId(), RejectReason.MALFORMED, "eventTime is required", -1);
        }
        if (r.scheduledTime() != null && r.scheduledTime().isAfter(r.eventTime())
                && EventType.STARTED.name().equals(r.eventType())) {
            return reject(r.eventId(), RejectReason.MALFORMED,
                    "scheduledTime " + r.scheduledTime() + " cannot be after STARTED eventTime "
                            + r.eventTime(), -1);
        }
        return null;
    }

    /**
     * 生命周期矛盾规则（仅基于 eventTime 与事件类型，与到达顺序无关）：
     * 1. 同一 run 只允许一个 STARTED、一个终态；重复即矛盾（重复上报应复用 eventId）。
     * 2. 终态类型必须唯一：SUCCEEDED/FAILED 不能并存。
     * 3. 当 STARTED 与终态都已知时，eventTime 不得倒挂（开始晚于结束）；
     *    若终态先到、STARTED 尚未到达，则 STARTED 迟到属于合法乱序，暂不拒绝。
     */
    private static String lifecycleConflict(RunState run, EventReport r, EventType type) {
        if (run == null || run.getEventIds().isEmpty()) {
            return null;
        }
        if (type == EventType.STARTED) {
            if (run.getStartedAt() != null) {
                return "run " + r.runId() + " already has STARTED at " + run.getStartedAt()
                        + "; a retried start must use a new runId (resend must reuse eventId)";
            }
            // STARTED 已知存在，迟到/新到的终态早于它才是倒挂；终态先到的情形不在此分支
            return null;
        }
        // 终态事件
        if (run.getTerminalType() != null) {
            if (run.getTerminalType() == type) {
                return "run " + r.runId() + " already reached terminal state " + type
                        + " at " + run.getFinishedAt() + "; duplicate terminal must reuse eventId";
            }
            return "run " + r.runId() + " already terminated as " + run.getTerminalType()
                    + " at " + run.getFinishedAt() + "; cannot also be " + type;
        }
        if (run.getStartedAt() != null && r.eventTime().isBefore(run.getStartedAt())) {
            return "terminal eventTime " + r.eventTime() + " is before existing STARTED at "
                    + run.getStartedAt() + " for run " + r.runId();
        }
        return null;
    }

    private static boolean samePayload(JobRunEvent e, EventReport r, EventType type) {
        return e.eventType() == type
                && e.pipelineId().equals(r.pipelineId())
                && e.runId().equals(r.runId())
                && e.eventTime().equals(r.eventTime())
                && java.util.Objects.equals(e.scheduledTime(), r.scheduledTime());
    }

    private static IngestResult reject(String eventId, RejectReason reason, String message, long seq) {
        return new IngestResult(IngestOutcome.REJECTED, eventId, reason, message, seq);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

}
