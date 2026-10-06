package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 在线规则更新的处理状态。 */
public enum RuleUpdateStatus {
    /** 校验通过，已整体生效。 */
    APPLIED,
    /** 校验失败，整条未生效。 */
    REJECTED
}
