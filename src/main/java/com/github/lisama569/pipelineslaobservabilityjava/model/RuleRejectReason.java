package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 在线规则更新被拒绝的可区分原因。 */
public enum RuleRejectReason {
    /** 流水线标识为空。 */
    INVALID_PIPELINE_ID,
    /** 评估窗口必须为正数。 */
    INVALID_WINDOW,
    /** 新鲜度阈值不得为负。 */
    INVALID_MAX_FRESHNESS_LAG,
    /** 调度延迟阈值不得为负。 */
    INVALID_MAX_SCHEDULE_DELAY,
    /** 达标率阈值必须落在 [0,1]。 */
    INVALID_MIN_SLA_ATTAINMENT,
    /** 恢复裕量必须落在 [0,1]。 */
    INVALID_RECOVERY_MARGIN,
    /** 升级次数必须 >= 1。 */
    INVALID_ESCALATE_AFTER
}
