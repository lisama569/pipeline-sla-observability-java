package com.github.lisama569.pipelineslaobservabilityjava.model;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;

/**
 * 在线规则更新的处理结果。
 *
 * @param status        处理状态
 * @param reason        拒绝原因（仅 REJECTED 时非空，可区分）
 * @param detail        人类可读的判定依据
 * @param effectiveRule 生效后的完整规则（APPLIED 时为更新后的快照）
 */
public record RuleUpdateResult(RuleUpdateStatus status, RuleRejectReason reason,
                               String detail, SlaProperties.PipelineRule effectiveRule) {

    public static RuleUpdateResult applied(SlaProperties.PipelineRule rule, String detail) {
        return new RuleUpdateResult(RuleUpdateStatus.APPLIED, null, detail, rule);
    }

    public static RuleUpdateResult rejected(RuleRejectReason reason, String detail) {
        return new RuleUpdateResult(RuleUpdateStatus.REJECTED, reason, detail, null);
    }
}
