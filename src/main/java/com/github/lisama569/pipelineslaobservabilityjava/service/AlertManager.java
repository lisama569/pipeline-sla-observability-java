package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.Severity;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 告警生命周期管理：触发、去重（滞回）、升级与自动恢复。
 *
 * <p>语义约定：
 * <ul>
 *   <li>告警键 = (pipelineId, metricType)；同一键同一时间最多一个 FIRING 告警，
 *       持续违约只更新既有告警（计数/升级），不产生重复告警。</li>
 *   <li>恢复采用滞回：指标需越过 阈值±recoveryMargin 才判定恢复，
 *       阈值附近抖动不会引起告警反复触发/恢复。</li>
 *   <li>连续违约评估次数达到 escalateAfter 后 WARNING 升级为 CRITICAL。</li>
 *   <li>恢复后再次违约生成新的 alertId，开启新的生命周期。</li>
 *   <li>告警时间取评估的事件时间水位线，保证并发/重放下状态变迁确定。</li>
 * </ul>
 * 所有状态迁移在本对象的监视器锁内完成，并发评估下不会丢失或倒退。
 */
@Service
public class AlertManager {

    /** 单个告警键的运行时状态。 */
    private static final class MetricState {
        String openAlertId;
        int consecutiveBreaches;
    }

    private final SlaProperties properties;
    private final Map<String, Alert> alertsById = new LinkedHashMap<>();
    private final Map<String, Map<MetricType, MetricState>> states = new LinkedHashMap<>();
    private long sequence;

    public AlertManager(SlaProperties properties) {
        this.properties = properties;
    }

    /** 根据一次评估结论推进告警生命周期。 */
    public synchronized void onAssessment(PipelineAssessment a) {
        if (a.evaluatedAt() == null) {
            return; // 无任何事件，不评估告警
        }
        SlaProperties.PipelineRule rule = properties.ruleFor(a.pipelineId());
        Map<MetricType, MetricState> metricStates =
                states.computeIfAbsent(a.pipelineId(), k -> new EnumMap<>(MetricType.class));

        applyMetric(a, rule, metricStates, MetricType.FRESHNESS,
                a.freshnessBreached(),
                a.freshnessLagSeconds() != null
                        && a.freshnessLagSeconds() <= rule.getMaxFreshnessLag().getSeconds() * (1 - rule.getRecoveryMargin()),
                "freshnessLagSeconds=" + a.freshnessLagSeconds());

        applyMetric(a, rule, metricStates, MetricType.SCHEDULE_DELAY,
                a.scheduleDelayBreached(),
                a.maxScheduleDelaySeconds() <= rule.getMaxScheduleDelay().getSeconds() * (1 - rule.getRecoveryMargin()),
                "maxScheduleDelaySeconds=" + a.maxScheduleDelaySeconds());

        applyMetric(a, rule, metricStates, MetricType.SLA_ATTAINMENT,
                a.slaBreached(),
                a.slaAttainment() != null
                        && a.slaAttainment() >= rule.getMinSlaAttainment() + rule.getRecoveryMargin(),
                "slaAttainment=" + a.slaAttainment());
    }

    private void applyMetric(PipelineAssessment a, SlaProperties.PipelineRule rule,
                             Map<MetricType, MetricState> metricStates, MetricType metric,
                             boolean breached, boolean recoveredWithMargin, String evidence) {
        MetricState state = metricStates.computeIfAbsent(metric, k -> new MetricState());
        String key = a.pipelineId() + "|" + metric;
        Instant at = a.evaluatedAt();

        if (breached) {
            state.consecutiveBreaches++;
            if (state.openAlertId == null) {
                String alertId = key + "|" + at + "|" + (++sequence);
                alertsById.put(alertId, new Alert(alertId, a.pipelineId(), metric,
                        Severity.WARNING, AlertStatus.FIRING, at, null, 0,
                        "触发: " + evidence));
                state.openAlertId = alertId;
            } else {
                Alert open = alertsById.get(state.openAlertId);
                if (open.severity() == Severity.WARNING
                        && state.consecutiveBreaches >= rule.getEscalateAfter()) {
                    alertsById.put(open.alertId(), new Alert(open.alertId(), open.pipelineId(), open.metricType(),
                            Severity.CRITICAL, AlertStatus.FIRING, open.firedAt(), null,
                            open.escalationCount() + 1, "升级: 连续违约 " + state.consecutiveBreaches + " 次, " + evidence));
                }
            }
        } else if (recoveredWithMargin) {
            state.consecutiveBreaches = 0;
            if (state.openAlertId != null) {
                Alert open = alertsById.get(state.openAlertId);
                alertsById.put(open.alertId(), new Alert(open.alertId(), open.pipelineId(), open.metricType(),
                        open.severity(), AlertStatus.RESOLVED, open.firedAt(), at,
                        open.escalationCount(), "自动恢复: " + evidence));
                state.openAlertId = null;
            }
        }
        // 既未违约也未带裕量恢复（阈值附近抖动或指标缺失）：保持现状，不计数不迁移。
    }

    /** 当前处于 FIRING 的告警。 */
    public synchronized List<Alert> activeAlerts() {
        return alertsById.values().stream()
                .filter(a -> a.status() == AlertStatus.FIRING)
                .toList();
    }

    /** 全部告警（含已恢复，每个 alertId 取最新快照）。 */
    public synchronized List<Alert> allAlerts() {
        return new ArrayList<>(alertsById.values());
    }
}
