package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 告警认领处理状态。 */
public enum AckStatus {
    /** 认领成功。 */
    ACKNOWLEDGED,
    /** 该告警已被认领，幂等忽略（无副作用）。 */
    DUPLICATE,
    /** 认领被拒绝。 */
    REJECTED
}
