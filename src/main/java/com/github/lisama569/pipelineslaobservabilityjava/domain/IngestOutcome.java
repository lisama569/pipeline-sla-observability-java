package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 上报处理结论：幂等接收 / 重复上报 / 拒绝。
 */
public enum IngestOutcome {
    ACCEPTED,
    DUPLICATE,
    REJECTED
}
