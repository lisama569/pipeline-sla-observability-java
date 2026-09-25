package com.github.lisama569.pipelineslaobservabilityjava.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个告警生命周期：(pipelineId, metricType) 同一时刻至多存在一个未恢复告警。
 * 可变实体，仅在所属流水线锁内访问。
 *
 * <p>去重与迟滞完全由 lastBreachAt / lastNormalAt 两个时间戳驱动：
 * 每次违约刷新 lastBreachAt，每次正常刷新 lastNormalAt；
 * 阈值附近来回抖动只会反复刷新违约时间，不会产生第二条告警。</p>
 */
public final class Alert {

    private final String id;
    private final String pipelineId;
    private final MetricType metricType;
    private AlertState state;
    private final Instant firedAt;
    private Instant escalatedAt;
    private Instant recoveredAt;
    private Double breachValue;
    private Double recoveryValue;
    private long triggerEvaluationSeq;
    /** 最近一次观测到违约的评估时刻（asOf） */
    private Instant lastBreachAt;
    /** 最近一次观测到达标的评估时刻（asOf） */
    private Instant lastNormalAt;
    private final List<AlertTransition> transitions = new ArrayList<>();

    public Alert(String id, String pipelineId, MetricType metricType,
                 Instant firedAt, Double breachValue, long triggerEvaluationSeq) {
        this.id = id;
        this.pipelineId = pipelineId;
        this.metricType = metricType;
        this.state = AlertState.FIRING;
        this.firedAt = firedAt;
        this.breachValue = breachValue;
        this.triggerEvaluationSeq = triggerEvaluationSeq;
        this.lastBreachAt = firedAt;
    }

    public String getId() { return id; }
    public String getPipelineId() { return pipelineId; }
    public MetricType getMetricType() { return metricType; }
    public AlertState getState() { return state; }
    public void setState(AlertState state) { this.state = state; }
    public Instant getFiredAt() { return firedAt; }
    public Instant getEscalatedAt() { return escalatedAt; }
    public void setEscalatedAt(Instant escalatedAt) { this.escalatedAt = escalatedAt; }
    public Instant getRecoveredAt() { return recoveredAt; }
    public void setRecoveredAt(Instant recoveredAt) { this.recoveredAt = recoveredAt; }
    public Double getBreachValue() { return breachValue; }
    public void setBreachValue(Double breachValue) { this.breachValue = breachValue; }
    public Double getRecoveryValue() { return recoveryValue; }
    public void setRecoveryValue(Double recoveryValue) { this.recoveryValue = recoveryValue; }
    public long getTriggerEvaluationSeq() { return triggerEvaluationSeq; }
    public Instant getLastBreachAt() { return lastBreachAt; }
    public void setLastBreachAt(Instant lastBreachAt) { this.lastBreachAt = lastBreachAt; }
    public Instant getLastNormalAt() { return lastNormalAt; }
    public void setLastNormalAt(Instant lastNormalAt) { this.lastNormalAt = lastNormalAt; }
    public List<AlertTransition> getTransitions() { return transitions; }
}
