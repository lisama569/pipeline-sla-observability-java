package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 维护窗口声明/撤销请求。metricType 为 null 表示针对整条流水线的全部指标。
 */
public record MaintenanceRequest(
        String pipelineId,
        MetricType metricType,
        Instant start,
        Instant end,
        String note) {
}
