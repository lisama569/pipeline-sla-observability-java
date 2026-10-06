package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 告警认领结果。
 *
 * @param status 处理状态
 * @param reason 拒绝原因（仅 REJECTED 时非空，可区分）
 * @param alert  认领后的告警快照（成功/幂等时非空）
 * @param detail 人类可读的判定依据
 */
public record AckResult(AckStatus status, AckRejectReason reason, Alert alert, String detail) {

    public static AckResult of(AckStatus status, Alert alert, String detail) {
        return new AckResult(status, null, alert, detail);
    }

    public static AckResult rejected(AckRejectReason reason, String detail) {
        return new AckResult(AckStatus.REJECTED, reason, null, detail);
    }
}
