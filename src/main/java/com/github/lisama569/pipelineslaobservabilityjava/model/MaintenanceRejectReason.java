package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 维护窗口声明被拒绝的可区分原因。 */
public enum MaintenanceRejectReason {
    /** 必填字段缺失或时间段非法（start 不早于 end）。 */
    INVALID_WINDOW,
    /** 同 windowId 已存在但内容不同。 */
    CONFLICTING_WINDOW
}
