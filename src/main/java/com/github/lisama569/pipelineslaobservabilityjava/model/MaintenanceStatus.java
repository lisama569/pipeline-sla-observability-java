package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 维护窗口声明/撤销的处理状态。 */
public enum MaintenanceStatus {
    /** 新声明已生效。 */
    DECLARED,
    /** 相同窗口已存在，幂等忽略（无副作用）。 */
    DUPLICATE,
    /** 撤销成功。 */
    REVOKED,
    /** 撤销目标不存在。 */
    NOT_FOUND,
    /** 入参非法，被拒绝。 */
    REJECTED
}
