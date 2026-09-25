package com.github.lisama569.pipelineslaobservabilityjava.evaluation;

import com.github.lisama569.pipelineslaobservabilityjava.alert.AlertService;
import com.github.lisama569.pipelineslaobservabilityjava.clock.Clock;
import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineObservabilityProperties;
import com.github.lisama569.pipelineslaobservabilityjava.domain.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricValue;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RunState;
import com.github.lisama569.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 确定性评估服务。
 *
 * <p><b>可复现</b>：结论是 (pipelineId, asOf, asOf 时刻前已落库事件集合) 的纯函数。
 * 同一组事件在同一 asOf 下，无论事件以何种顺序到达，结论完全一致。事件按
 * (eventTime, ingestSeq) 全序排列，ingestSeq 打破同刻事件的平局，与墙上时钟无关。</p>
 *
 * <p><b>可追溯</b>：每次评估落一条 {@link PipelineEvaluation}，记录 asOf、窗口、
 * 各指标判定依据 detail 以及参与计算的事件序号 basedOnEventSeqs；历史结论因迟到事件
 * 而变化时，前后两次评估及其输入事件序列可直接对比。</p>
 *
 * <p><b>开销有界</b>：评估只扫描 retention 与窗口范围内的事件，且每次评估顺带裁剪
 * 早于 retentionCutoff 的事件/运行；历史结论按配置条数环形淘汰，不随历史无限增长。</p>
 */
@Service
public class EvaluationService {

    private static final Logger log = LoggerFactory.getLogger(EvaluationService.class);

    private final PipelineRegistry registry;
    private final Clock clock;
    private final AlertService alertService;
    private final PipelineObservabilityProperties properties;

    public EvaluationService(PipelineRegistry registry, Clock clock, AlertService alertService,
                             PipelineObservabilityProperties properties) {
        this.registry = registry;
        this.clock = clock;
        this.alertService = alertService;
        this.properties = properties;
    }

    public PipelineEvaluation evaluate(String pipelineId) {
        PipelineState state = registry.find(pipelineId)
                .orElseThrow(() -> new IllegalArgumentException("pipeline not found: " + pipelineId));
        synchronized (state) {
            return evaluateWithinLock(state, clock.now());
        }
    }

    public PipelineEvaluation evaluateAt(String pipelineId, Instant asOf) {
        if (asOf == null) {
            throw new IllegalArgumentException("asOf is required");
        }
        PipelineState state = registry.find(pipelineId)
                .orElseThrow(() -> new IllegalArgumentException("pipeline not found: " + pipelineId));
        synchronized (state) {
            // 指定历史时刻的重放：只读，不推进告警、不裁剪、不占用评估序号
            return computeEvaluation(state, asOf, false);
        }
    }

    public List<PipelineEvaluation> evaluateAll() {
        List<PipelineEvaluation> results = new ArrayList<>();
        Instant now = clock.now();
        for (PipelineState state : registry.all()) {
            synchronized (state) {
                results.add(evaluateWithinLock(state, now));
            }
        }
        return results;
    }

    /**
     * 在已持有流水线锁的前提下执行一次正式评估：计算、裁剪、留痕、对账告警。
     * 上报路径调用本方法，保证“落库-评估-告警”在同一把锁内原子完成。
     */
    public PipelineEvaluation evaluateWithinLock(PipelineState state, Instant asOf) {
        PipelineEvaluation evaluation = computeEvaluation(state, asOf, true);
        pruneExpired(state, asOf);
        appendEvaluation(state, evaluation);
        alertService.reconcile(state, evaluation);
        return evaluation;
    }

    /**
     * 纯计算：给定 state 与 asOf 计算一次结论。persist=false 时为历史重放。
     */
    PipelineEvaluation computeEvaluation(PipelineState state, Instant asOf, boolean persist) {
        PipelineConfig config = state.getConfig();
        Instant windowStart = asOf.minusSeconds(config.windowSeconds());

        List<JobRunEvent> visible = new ArrayList<>();
        for (var entry : state.getOrderedEvents()
                .headMap(new PipelineState.EventOrderKey(asOf, Long.MAX_VALUE), true)
                .entrySet()) {
            if (!entry.getKey().eventTime().isBefore(windowStart)) {
                visible.add(entry.getValue());
            }
        }

        List<Long> basedOn = visible.stream().map(JobRunEvent::ingestSeq).sorted().toList();

        List<MetricValue> metrics = new ArrayList<>(3);
        metrics.add(evaluateFreshness(state, config, visible, asOf));
        metrics.add(evaluateScheduleDelay(config, visible));
        metrics.add(evaluateFailureRate(config, visible));

        boolean anyBreach = metrics.stream().anyMatch(MetricValue::breached);
        boolean anyObserved = metrics.stream().anyMatch(MetricValue::observed);
        SlaStatus slaStatus = anyBreach ? SlaStatus.BREACH
                : anyObserved ? SlaStatus.PASS : SlaStatus.UNKNOWN;

        long seq = persist ? state.nextEvaluationSeq()
                : (state.getEvaluationSeq() == 0 ? 0 : state.getEvaluationSeq());
        PipelineEvaluation evaluation = new PipelineEvaluation(
                config.pipelineId(), seq, asOf, windowStart,
                List.copyOf(metrics), slaStatus, basedOn, persist ? clock.now() : asOf);
        log.info("evaluation computed pipeline={} seq={} asOf={} sla={} basedOnSeqs={} metrics={}",
                config.pipelineId(), seq, asOf, slaStatus, basedOn, metrics);
        return evaluation;
    }

    /**
     * 新鲜度 = asOf - 窗口内最近一次“终态事件时间”。
     * 注意按事件实际发生时间（eventTime）而非接收时间计算，迟到事件纳入后可能使历史结论
     * 从 BREACH 变为 PASS——该变化通过相邻两次评估记录可追溯。
     */
    private MetricValue evaluateFreshness(PipelineState state, PipelineConfig config,
                                          List<JobRunEvent> events, Instant asOf) {
        Thresholds t = config.thresholds();
        Instant lastFinish = null;
        JobRunEvent lastFinishEvent = null;
        for (JobRunEvent e : events) {
            if ((e.eventType() == EventType.SUCCEEDED || e.eventType() == EventType.FAILED)
                    && (lastFinish == null || !e.eventTime().isBefore(lastFinish))) {
                lastFinish = e.eventTime();
                lastFinishEvent = e;
            }
        }
        if (lastFinishEvent == null) {
            return new MetricValue(MetricType.FRESHNESS, false, null,
                    t.maxFreshnessSeconds(), false,
                    "no terminal event within window [" + events.size()
                            + " event(s)]; freshness unobserved");
        }
        long seconds = Duration.between(lastFinish, asOf).getSeconds();
        boolean breached = seconds > t.maxFreshnessSeconds();
        return new MetricValue(MetricType.FRESHNESS, true, (double) seconds,
                t.maxFreshnessSeconds(), breached,
                "latest terminal event " + lastFinishEvent.eventId() + " ("
                        + lastFinishEvent.eventType() + ") at " + lastFinish
                        + ", age=" + seconds + "s, threshold<=" + t.maxFreshnessSeconds()
                        + "s => " + (breached ? "BREACH" : "OK"));
    }

    /**
     * 调度延迟 = STARTED.eventTime - STARTED.scheduledTime，取窗口内最大值。
     * 仅有携带 scheduledTime 的 STARTED 事件才参与；无观测则不判定（不误报）。
     */
    private MetricValue evaluateScheduleDelay(PipelineConfig config, List<JobRunEvent> events) {
        Thresholds t = config.thresholds();
        long maxDelay = Long.MIN_VALUE;
        JobRunEvent maxEvent = null;
        int sampled = 0;
        for (JobRunEvent e : events) {
            if (e.eventType() == EventType.STARTED && e.scheduledTime() != null) {
                sampled++;
                long delay = Duration.between(e.scheduledTime(), e.eventTime()).getSeconds();
                if (delay >= maxDelay) {
                    maxDelay = delay;
                    maxEvent = e;
                }
            }
        }
        if (maxEvent == null) {
            return new MetricValue(MetricType.SCHEDULE_DELAY, false, null,
                    t.maxScheduleDelaySeconds(), false,
                    "no STARTED event with scheduledTime within window (" + sampled
                            + " started sampled); delay unobserved");
        }
        boolean breached = maxDelay > t.maxScheduleDelaySeconds();
        return new MetricValue(MetricType.SCHEDULE_DELAY, true, (double) maxDelay,
                t.maxScheduleDelaySeconds(), breached,
                "max delay from STARTED " + maxEvent.eventId() + ": scheduled "
                        + maxEvent.scheduledTime() + " -> started " + maxEvent.eventTime()
                        + ", delay=" + maxDelay + "s, threshold<=" + t.maxScheduleDelaySeconds()
                        + "s => " + (breached ? "BREACH" : "OK"));
    }

    /**
     * 失败率 = 窗口内失败终态事件数 / 全部终态事件数 × 100。
     * 无终态事件时不判定。
     */
    private MetricValue evaluateFailureRate(PipelineConfig config, List<JobRunEvent> events) {
        Thresholds t = config.thresholds();
        int total = 0;
        int failed = 0;
        JobRunEvent lastFailed = null;
        for (JobRunEvent e : events) {
            if (e.eventType() == EventType.SUCCEEDED || e.eventType() == EventType.FAILED) {
                total++;
                if (e.eventType() == EventType.FAILED) {
                    failed++;
                    lastFailed = e;
                }
            }
        }
        if (total == 0) {
            return new MetricValue(MetricType.FAILURE_RATE, false, null,
                    t.maxFailureRatePercent(), false,
                    "no terminal event within window; failure rate unobserved");
        }
        double rate = failed * 100.0d / total;
        // 严格大于阈值才违约，避免 0% 阈值下任何非失败结果抖动
        boolean breached = rate > t.maxFailureRatePercent();
        return new MetricValue(MetricType.FAILURE_RATE, true, rate,
                t.maxFailureRatePercent(), breached,
                "failed=" + failed + "/" + total + " runs, rate="
                        + String.format("%.2f", rate) + "%, threshold<="
                        + t.maxFailureRatePercent() + "%"
                        + (lastFailed != null ? ", last failed event=" + lastFailed.eventId() : "")
                        + " => " + (breached ? "BREACH" : "OK"));
    }

    /** 裁剪早于 retention 窗口的事件；完全过期的 run 同步移除，保证内存开销有界。 */
    private void pruneExpired(PipelineState state, Instant asOf) {
        PipelineConfig config = state.getConfig();
        Instant cutoff = asOf.minusSeconds(config.retentionSeconds());
        var expired = state.getOrderedEvents()
                .headMap(new PipelineState.EventOrderKey(cutoff, Long.MIN_VALUE), false);
        if (expired.isEmpty()) {
            return;
        }
        int removed = 0;
        for (JobRunEvent e : new ArrayList<>(expired.values())) {
            state.getEventsById().remove(e.eventId());
            removed++;
        }
        expired.clear();
        var runIterator = state.getRuns().values().iterator();
        while (runIterator.hasNext()) {
            RunState run = runIterator.next();
            if (run.getEventIds().stream().noneMatch(state.getEventsById()::containsKey)) {
                runIterator.remove();
            }
        }
        log.debug("pruned {} event(s) older than {} for pipeline {}", removed, cutoff,
                config.pipelineId());
    }

    private void appendEvaluation(PipelineState state, PipelineEvaluation evaluation) {
        var deque = state.getEvaluations();
        deque.addLast(evaluation);
        int limit = Math.max(1, properties.getEvaluationHistoryLimit());
        while (deque.size() > limit) {
            deque.removeFirst();
        }
    }
}
