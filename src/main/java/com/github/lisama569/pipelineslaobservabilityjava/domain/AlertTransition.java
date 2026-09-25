package com.github.lisama569.pipelineslaobservabilityjava.domain;

import java.time.Instant;

/**
 * 告警生命周期迁移记录（任何状态变化都留痕，可解释、可追溯）。
 *
 * @param at          迁移发生时间
 * @param from        迁移前状态（首次触发时为 null）
 * @param to          迁移后状态
 * @param reason      迁移原因（TRIGGERED / ESCALATED_BY_DURATION / AUTO_RECOVERED / MANUAL_RECOVERED）
 * @param metricValue 迁移时指标值
 * @param evaluationSeq 触发该迁移的评估序号
 */
public record AlertTransition(
        Instant at,
        AlertState from,
        AlertState to,
        String reason,
        Double metricValue,
        long evaluationSeq
) {
}
