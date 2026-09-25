package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 被评估的指标类型，同时作为告警去重的维度。
 */
public enum MetricType {
    /** 新鲜度：最近一次完成距评估时刻的秒数 */
    FRESHNESS,
    /** 调度延迟：实际开始时间相对计划时间的延迟秒数 */
    SCHEDULE_DELAY,
    /** 失败率：窗口内失败终态占比（百分比） */
    FAILURE_RATE
}
