package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Duration;

/**
 * 在线规则更新请求（不可变）。所有字段可空：null 表示该项不修改，
 * 非 null 项先整体校验，全部通过才一次性生效（不允许只改一半）。
 */
public record RuleUpdateRequest(
        Duration window,
        Duration maxFreshnessLag,
        Duration maxScheduleDelay,
        Double minSlaAttainment,
        Double recoveryMargin,
        Integer escalateAfter) {

    public boolean isEmpty() {
        return window == null && maxFreshnessLag == null && maxScheduleDelay == null
                && minSlaAttainment == null && recoveryMargin == null && escalateAfter == null;
    }
}
