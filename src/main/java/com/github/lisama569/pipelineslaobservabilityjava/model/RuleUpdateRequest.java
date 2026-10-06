package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Duration;

/**
 * 在线规则更新请求：所有字段可空，null 表示该项保持不变。
 * 校验采用全量前置：任一字段非法则整条不生效。
 */
public record RuleUpdateRequest(
        Duration window,
        Duration maxFreshnessLag,
        Duration maxScheduleDelay,
        Double minSlaAttainment,
        Double recoveryMargin,
        Integer escalateAfter) {
}
