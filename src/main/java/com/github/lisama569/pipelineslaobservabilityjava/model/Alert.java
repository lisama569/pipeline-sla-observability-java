package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 告警（不可变快照；生命周期变迁由 AlertManager 产生新快照并追加历史）。
 *
 * @param acknowledgedBy 认领人（未认领为 null）；认领信息在本次告警生命周期内一直保留
 * @param ackNote        认领备注
 * @param acknowledgedAt 认领时间
 */
public record Alert(
        String alertId,
        String pipelineId,
        MetricType metricType,
        Severity severity,
        AlertStatus status,
        Instant firedAt,
        Instant resolvedAt,
        int escalationCount,
        String detail,
        String acknowledgedBy,
        String ackNote,
        Instant acknowledgedAt) {

    /** 兼容构造：未认领的告警。 */
    public Alert(String alertId, String pipelineId, MetricType metricType, Severity severity,
                 AlertStatus status, Instant firedAt, Instant resolvedAt, int escalationCount, String detail) {
        this(alertId, pipelineId, metricType, severity, status, firedAt, resolvedAt,
                escalationCount, detail, null, null, null);
    }

    /** 是否已被认领。 */
    public boolean acknowledged() {
        return acknowledgedBy != null;
    }
}
