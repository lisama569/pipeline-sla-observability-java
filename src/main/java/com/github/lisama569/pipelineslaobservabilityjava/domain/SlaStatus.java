package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 流水线级 SLA 判定。
 */
public enum SlaStatus {
    /** 窗口内指标全部达标 */
    PASS,
    /** 至少一项指标违约 */
    BREACH,
    /** 数据不足，无法判定 */
    UNKNOWN
}
