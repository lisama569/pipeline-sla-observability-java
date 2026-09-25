package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 告警（不可变快照；生命周期变迁由 AlertManager 产生新快照并追加历史）。
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
        String detail) {
}
