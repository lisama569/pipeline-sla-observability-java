package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 在线规则更新被拒绝的可区分原因。 */
public enum RuleRejectReason {
    /** pipelineId 缺失。 */
    MISSING_PIPELINE_ID,
    /** 请求未携带任何待修改字段。 */
    NO_FIELDS,
    /** 评估窗口必须为正数。 */
    INVALID_WINDOW,
    /** 新鲜度阈值不能为负。 */
    INVALID_FRESHNESS_LAG,
    /** 调度延迟阈值不能为负。 */
    INVALID_SCHEDULE_DELAY,
    /** 达标率阈值必须在 [0,1]。 */
    INVALID_ATTAINMENT,
    /** 恢复裕量必须在 [0,1]。 */
    INVALID_RECOVERY_MARGIN,
    /** 升级次数必须 >= 1。 */
    INVALID_ESCALATE_AFTER
}
