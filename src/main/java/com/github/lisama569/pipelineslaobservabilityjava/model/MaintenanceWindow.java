package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 计划内维护窗口（不可变）。窗口按事件时间口径判断：[start, end)。
 *
 * @param windowId   幂等键：pipelineId|metric(或 ALL)|start|end，重复声明得到同一 windowId
 * @param pipelineId 流水线标识
 * @param metricType 仅静默的指标；为 null 表示静默该流水线的全部指标
 * @param start      维护开始（事件时间，含）
 * @param end        维护结束（事件时间，不含）
 * @param note       备注（可选）
 */
public record MaintenanceWindow(
        String windowId,
        String pipelineId,
        MetricType metricType,
        Instant start,
        Instant end,
        String note) {

    /** 计算幂等键。 */
    public static String keyOf(String pipelineId, MetricType metricType, Instant start, Instant end) {
        return pipelineId + "|" + (metricType == null ? "ALL" : metricType.name()) + "|" + start + "|" + end;
    }

    /** 该窗口是否在事件时间 at 静默给定指标。 */
    public boolean covers(MetricType metric, Instant at) {
        boolean metricMatch = metricType == null || metricType == metric;
        return metricMatch && !at.isBefore(start) && at.isBefore(end);
    }
}
