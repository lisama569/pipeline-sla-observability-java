package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 上报被拒绝的可区分原因。 */
public enum RejectReason {
    /** 必填字段缺失或非法。 */
    INVALID_EVENT,
    /** 流水线未注册。 */
    UNKNOWN_PIPELINE,
    /** 与既有记录矛盾（同一去重键、不同内容）。 */
    CONFLICTING_EVENT,
    /** FINISHED 事件缺少终态。 */
    MISSING_STATUS,
    /** 事件时间早于已确认的同运行后续事件（时钟回拨矛盾）。 */
    EVENT_TIME_REGRESSION,
    /** 事件时间早于保留窗口下界（过迟到，相关历史已清理）。 */
    BEYOND_RETENTION
}
