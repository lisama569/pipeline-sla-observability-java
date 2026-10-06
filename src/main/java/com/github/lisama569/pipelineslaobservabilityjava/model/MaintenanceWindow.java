package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 计划内维护 / 静默窗口（不可变）。
 *
 * @param windowId   声明方提供的窗口标识（幂等去重键）
 * @param pipelineId 作用的流水线
 * @param metricType 作用的指标；为 null 表示该流水线的全部指标
 * @param start      维护开始（事件时间口径，含）
 * @param end        维护结束（事件时间口径，不含）
 * @param reason     人类可读的维护原因
 */
public record MaintenanceWindow(
        String windowId,
        String pipelineId,
        MetricType metricType,
        Instant start,
        Instant end,
        String reason) {

    /** 判断事件时间 at 是否落在本窗口内且作用于指定指标（[start, end) 区间）。 */
    public boolean covers(String pipeline, MetricType metric, Instant at) {
        return pipelineId.equals(pipeline)
                && (metricType == null || metricType == metric)
                && !at.isBefore(start) && at.isBefore(end);
    }
}
