package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 告警认领请求。
 *
 * @param ackBy 认领人（必填）
 * @param note  备注（可选）
 */
public record AckRequest(String ackBy, String note) {
}
