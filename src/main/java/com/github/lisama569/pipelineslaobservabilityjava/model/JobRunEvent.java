package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 作业运行上报事件（不可变）。
 *
 * @param pipelineId   流水线标识
 * @param runId        运行标识（同一运行的多次上报通过 pipelineId+runId+eventType 去重）
 * @param eventType    事件类型
 * @param status       终态（仅 FINISHED 事件需要）
 * @param scheduledAt  计划调度时间
 * @param occurredAt   事件发生时间（事件时间，评估只依赖该时间，不依赖上报到达时间）
 */
public record JobRunEvent(
        String pipelineId,
        String runId,
        EventType eventType,
        RunStatus status,
        Instant scheduledAt,
        Instant occurredAt) {
}
