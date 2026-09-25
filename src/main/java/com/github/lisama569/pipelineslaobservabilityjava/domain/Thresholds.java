package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 单条流水线的评估阈值与告警迟滞参数。
 *
 * @param maxFreshnessSeconds      新鲜度上限（最近完成距今超过即违约）
 * @param maxScheduleDelaySeconds  调度延迟上限（开始晚于计划超过即违约）
 * @param maxFailureRatePercent    窗口失败率上限（0~100，大于即违约）
 * @param escalationDelaySeconds   告警持续 FIRING 多久后升级
 * @param recoveryDelaySeconds     指标连续恢复多久后告警自动恢复
 */
public record Thresholds(
        long maxFreshnessSeconds,
        long maxScheduleDelaySeconds,
        double maxFailureRatePercent,
        long escalationDelaySeconds,
        long recoveryDelaySeconds
) {
}
