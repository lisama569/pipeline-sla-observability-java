package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 告警生命周期状态，只允许 FIRING -> ESCALATED -> RECOVERED（或 FIRING -> RECOVERED）单向迁移。
 */
public enum AlertState {
    FIRING,
    ESCALATED,
    RECOVERED
}
