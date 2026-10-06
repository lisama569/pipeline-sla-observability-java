package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 告警（不可变快照；生命周期变迁由 AlertManager 产生新快照并追加历史）。
 *
 * <p>认领信息（acknowledgedBy/acknowledgedAt/ackNote）挂在本次告警上直到恢复；
 * 恢复后再次违约会生成全新 alertId，认领状态重新计算（默认未认领）。
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
        Instant acknowledgedAt,
        String ackNote) {

    /** 未认领告警的构造便捷方法。 */
    public static Alert unacked(String alertId, String pipelineId, MetricType metricType,
                                Severity severity, AlertStatus status,
                                Instant firedAt, Instant resolvedAt,
                                int escalationCount, String detail) {
        return new Alert(alertId, pipelineId, metricType, severity, status,
                firedAt, resolvedAt, escalationCount, detail, null, null, null);
    }

    /** 复制本快照并替换指定字段以外的内容（用于状态迁移时携带认领信息）。 */
    public Alert with(Severity newSeverity, AlertStatus newStatus, Instant newResolvedAt,
                      int newEscalationCount, String newDetail) {
        return new Alert(alertId, pipelineId, metricType, newSeverity, newStatus,
                firedAt, newResolvedAt, newEscalationCount, newDetail,
                acknowledgedBy, acknowledgedAt, ackNote);
    }

    /** 复制本快照并附上认领信息。 */
    public Alert withAck(String by, Instant at, String note) {
        return new Alert(alertId, pipelineId, metricType, severity, status,
                firedAt, resolvedAt, escalationCount, detail, by, at, note);
    }
}
