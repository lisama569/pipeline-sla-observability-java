package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 告警认领的处理结果。
 *
 * @param status 处理状态（各取值可区分：成功 / 幂等重复 / 不存在 / 已恢复拒绝）
 * @param alert  认领后的告警快照（未找到时为 null）
 * @param detail 人类可读的判定依据
 */
public record AckResult(AckStatus status, Alert alert, String detail) {
}
