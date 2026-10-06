package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 在线规则更新结果。
 *
 * @param status 处理状态
 * @param reason 拒绝原因（仅 REJECTED 时非空，可区分）
 * @param detail 人类可读的判定依据
 */
public record RuleUpdateResult(RuleUpdateStatus status, RuleRejectReason reason, String detail) {

    public static RuleUpdateResult applied(String detail) {
        return new RuleUpdateResult(RuleUpdateStatus.APPLIED, null, detail);
    }

    public static RuleUpdateResult rejected(RuleRejectReason reason, String detail) {
        return new RuleUpdateResult(RuleUpdateStatus.REJECTED, reason, detail);
    }
}
