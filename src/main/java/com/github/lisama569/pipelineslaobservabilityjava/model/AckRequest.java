package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;

/**
 * 告警认领请求。
 *
 * @param operator       认领人（必填）
 * @param note           认领备注（可选）
 * @param acknowledgedAt 认领时间（可选，缺省由服务端填充）
 */
public record AckRequest(String operator, String note, Instant acknowledgedAt) {
}
