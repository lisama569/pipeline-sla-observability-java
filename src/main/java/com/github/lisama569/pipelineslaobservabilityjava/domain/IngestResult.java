package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 单次上报的处理结果。
 *
 * @param outcome   ACCEPTED / DUPLICATE / REJECTED
 * @param eventId   事件幂等键
 * @param reason    被拒绝时的可区分原因，其余情况为 null
 * @param message   人读判定依据
 * @param ingestSeq 服务端接收序号（重复上报返回首次接收时的序号）
 */
public record IngestResult(
        IngestOutcome outcome,
        String eventId,
        RejectReason reason,
        String message,
        long ingestSeq
) {
}
