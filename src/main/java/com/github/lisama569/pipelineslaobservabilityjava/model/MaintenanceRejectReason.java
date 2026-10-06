package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 维护窗口操作被拒绝的可区分原因。 */
public enum MaintenanceRejectReason {
    /** 必填字段缺失（pipelineId/start/end）。 */
    MISSING_FIELDS,
    /** 时间段非法（end 必须严格晚于 start）。 */
    INVALID_TIME_RANGE
}
