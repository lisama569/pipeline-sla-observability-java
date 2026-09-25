package com.github.lisama569.pipelineslaobservabilityjava.domain;

import java.time.Instant;

/**
 * 作业运行事件（服务端落库形态）。
 *
 * @param pipelineId    流水线标识
 * @param runId         同一次运行的标识（STARTED/终态共享）
 * @param eventId       事件幂等键，全局唯一
 * @param eventType     事件类型
 * @param eventTime     事件实际发生时间（由上报方时钟产生）
 * @param scheduledTime 计划调度时间，仅 STARTED 需要，可空
 * @param receivedAt    服务端接收时间
 * @param ingestSeq     服务端单调递增接收序号
 */
public record JobRunEvent(
        String pipelineId,
        String runId,
        String eventId,
        EventType eventType,
        Instant eventTime,
        Instant scheduledTime,
        Instant receivedAt,
        long ingestSeq
) {
}
