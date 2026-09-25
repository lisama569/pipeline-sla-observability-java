package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 上报处理结果。
 *
 * @param status 处理状态
 * @param reason 拒绝原因（仅 REJECTED 时非空）
 * @param detail 人类可读的判定依据
 */
public record IngestResult(IngestStatus status, RejectReason reason, String detail) {

    public static IngestResult accepted(String detail) {
        return new IngestResult(IngestStatus.ACCEPTED, null, detail);
    }

    public static IngestResult duplicate(String detail) {
        return new IngestResult(IngestStatus.DUPLICATE, null, detail);
    }

    public static IngestResult rejected(RejectReason reason, String detail) {
        return new IngestResult(IngestStatus.REJECTED, reason, detail);
    }
}
