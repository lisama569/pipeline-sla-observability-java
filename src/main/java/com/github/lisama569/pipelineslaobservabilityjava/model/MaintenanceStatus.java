package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 维护窗口声明 / 撤销的处理状态。 */
public enum MaintenanceStatus {
    /** 新窗口已登记。 */
    DECLARED,
    /** 同 id 同内容重复声明，幂等忽略。 */
    DUPLICATE,
    /** 窗口已撤销。 */
    REVOKED,
    /** 撤销目标不存在。 */
    NOT_FOUND,
    /** 声明被拒绝。 */
    REJECTED
}
