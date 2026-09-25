package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 上报被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 必填字段缺失或非法 */
    MALFORMED,
    /** 流水线未注册 */
    UNREGISTERED_PIPELINE,
    /** eventTime 超出允许的未来时间容差 */
    FUTURE_EVENT_TIME,
    /** 事件晚于可追溯窗口（迟到超出保留范围） */
    EXPIRED,
    /** 与既有记录矛盾：同一 eventId 载荷不同，或运行生命周期冲突 */
    CONFLICT
}
