package com.github.lisama569.pipelineslaobservabilityjava.ingestion;

import java.time.Instant;

/**
 * 作业运行事件的原始上报载荷（反序列化形态）。
 * eventType 故意保留为字符串，以便对非法取值给出 MALFORMED 而非框架 400。
 */
public record EventReport(
        String pipelineId,
        String runId,
        String eventId,
        String eventType,
        Instant eventTime,
        Instant scheduledTime
) {
}
