package com.github.lisama569.pipelineslaobservabilityjava.alert;

import com.github.lisama569.pipelineslaobservabilityjava.clock.Clock;
import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineObservabilityProperties;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.AlertState;
import com.github.lisama569.pipelineslaobservabilityjava.domain.AlertTransition;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricValue;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 告警生命周期管理（全部方法仅在所属流水线锁内调用，与上报、评估串行）。
 *
 * <p>对每条流水线的每个指标分别对账，规则：</p>
 * <ol>
 *   <li><b>触发去重</b>：某指标不存在未恢复告警且本次违约 → 新建 FIRING；
 *       已存在未恢复告警时，本次违约只刷新 lastBreachAt，绝不重复触发。</li>
 *   <li><b>阈值抖动</b>：违约/达标来回切换只更新时间戳。告警只有在
 *       asOf - lastBreachAt ≥ recoveryDelaySeconds（即连续恢复观察期已满足）时才自动恢复；
 *       抖动会在观察期内再次刷新 lastBreachAt，恢复计时作废。</li>
 *   <li><b>升级</b>：FIRING 且 asOf - firedAt ≥ escalationDelaySeconds 且仍处违约中
 *       → ESCALATED（只升一次）。</li>
 *   <li><b>新生命周期</b>：RECOVERED 告警移入有界历史；之后再次违约会触发全新 Alert，
 *       拥有独立 id 与完整迁移记录。</li>
 *   <li><b>无观测</b>：指标 UNKNOWN 不触发也不恢复告警，避免数据缺口造成误报/误恢复。</li>
 * </ol>
 */
@Service
public class AlertService {

    public static final String REASON_TRIGGERED = "TRIGGERED";
    public static final String REASON_ESCALATED = "ESCALATED_BY_DURATION";
    public static final String REASON_AUTO_RECOVERED = "AUTO_RECOVERED";
    public static final String REASON_MANUAL_RECOVERED = "MANUAL_RECOVERED";

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    private final Clock clock;
    private final PipelineObservabilityProperties properties;

    public AlertService(Clock clock, PipelineObservabilityProperties properties) {
        this.clock = clock;
        this.properties = properties;
    }

    /** 根据一次评估结论对账所有指标的告警。 */
    public void reconcile(PipelineState state, PipelineEvaluation evaluation) {
        Thresholds thresholds = state.getConfig().thresholds();
        for (MetricValue metric : evaluation.metrics()) {
            reconcileOne(state, evaluation, metric, thresholds);
        }
    }

    /** 手动恢复一条未恢复告警（运维接口预留），不允许跨状态倒退。 */
    public Alert manualRecover(PipelineState state, String alertId) {
        for (Alert alert : state.getActiveAlerts().values()) {
            if (alert.getId().equals(alertId)) {
                recover(state, alert, clock.now(), alert.getBreachValue(),
                        REASON_MANUAL_RECOVERED, state.getEvaluationSeq());
                return alert;
            }
        }
        throw new IllegalArgumentException("active alert not found: " + alertId);
    }

    private void reconcileOne(PipelineState state, PipelineEvaluation evaluation,
                              MetricValue metric, Thresholds thresholds) {
        MetricType type = metric.metricType();
        Instant asOf = evaluation.asOf();
        Alert active = state.getActiveAlerts().get(type);

        if (!metric.observed()) {
            // 数据缺口：保持现状，仅在告警已存在时不改变其计时
            if (active != null) {
                log.debug("metric {} unobserved at {}; alert {} left unchanged",
                        type, asOf, active.getId());
            }
            return;
        }

        if (metric.breached()) {
            if (active == null) {
                trigger(state, type, asOf, metric.value(), evaluation.seq());
            } else {
                // 同一违约生命周期：刷新违约时间、记录最新违约值，不产生新告警
                active.setLastBreachAt(asOf);
                active.setBreachValue(metric.value());
                maybeEscalate(active, asOf, metric.value(), evaluation.seq(), thresholds);
            }
            return;
        }

        // 本次达标
        if (active == null) {
            return;
        }
        active.setLastNormalAt(asOf);
        long sinceBreach = Duration.between(active.getLastBreachAt(), asOf).getSeconds();
        if (sinceBreach >= thresholds.recoveryDelaySeconds()) {
            recover(state, active, asOf, metric.value(),
                    REASON_AUTO_RECOVERED, evaluation.seq());
        } else {
            log.info("alert {} metric {} normal but within recovery hold ({}s < {}s); keep {}",
                    active.getId(), type, sinceBreach, thresholds.recoveryDelaySeconds(),
                    active.getState());
        }
    }

    private void trigger(PipelineState state, MetricType type, Instant at,
                         Double value, long evaluationSeq) {
        String alertId = state.getConfig().pipelineId() + ":" + type + ":"
                + UUID.randomUUID();
        Alert alert = new Alert(alertId, state.getConfig().pipelineId(), type, at, value, evaluationSeq);
        alert.getTransitions().add(new AlertTransition(
                at, null, AlertState.FIRING, REASON_TRIGGERED, value, evaluationSeq));
        state.getActiveAlerts().put(type, alert);
        log.warn("ALERT TRIGGERED {} pipeline={} metric={} value={} evalSeq={}",
                alertId, state.getConfig().pipelineId(), type, value, evaluationSeq);
    }

    private void maybeEscalate(Alert alert, Instant asOf, Double value,
                               long evaluationSeq, Thresholds thresholds) {
        // 升级只取决于“告警自触发以来持续未恢复的时长”；本次评估是否仍处违约状态已由
        // reconcileOne 在调用前确认（lastBreachAt 也刚被刷新）。
        if (alert.getState() == AlertState.FIRING
                && !asOf.isBefore(alert.getFiredAt().plusSeconds(thresholds.escalationDelaySeconds()))) {
            alert.setState(AlertState.ESCALATED);
            alert.setEscalatedAt(asOf);
            alert.getTransitions().add(new AlertTransition(
                    asOf, AlertState.FIRING, AlertState.ESCALATED,
                    REASON_ESCALATED, value, evaluationSeq));
            log.warn("ALERT ESCALATED {} metric={} at {} (breach lasted >= {}s)",
                    alert.getId(), alert.getMetricType(), asOf,
                    thresholds.escalationDelaySeconds());
        }
    }

    private void recover(PipelineState state, Alert alert, Instant at, Double value,
                         String reason, long evaluationSeq) {
        AlertState from = alert.getState();
        if (from == AlertState.RECOVERED) {
            // 状态倒退保护：恢复过的告警不会被再次恢复
            return;
        }
        alert.setState(AlertState.RECOVERED);
        alert.setRecoveredAt(at);
        alert.setRecoveryValue(value);
        alert.getTransitions().add(new AlertTransition(
                at, from, AlertState.RECOVERED, reason, value, evaluationSeq));
        state.getActiveAlerts().remove(alert.getMetricType());
        var recovered = state.getRecoveredAlerts();
        recovered.addLast(alert);
        int limit = Math.max(1, properties.getRecoveredAlertLimit());
        while (recovered.size() > limit) {
            recovered.removeFirst();
        }
        log.warn("ALERT RECOVERED {} metric={} reason={} at {} evalSeq={}",
                alert.getId(), alert.getMetricType(), reason, at, evaluationSeq);
    }
}
