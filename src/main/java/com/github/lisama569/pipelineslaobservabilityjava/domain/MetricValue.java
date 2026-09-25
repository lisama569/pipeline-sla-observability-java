package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 单个指标在某次评估中的取值与判定。
 *
 * @param metricType 指标类型
 * @param observed   是否有足够观测数据（false 时 value 为 null 且不参与违约判定）
 * @param value      指标当前值，无观测时为 null
 * @param threshold  适用阈值
 * @param breached   是否违约（observed=false 时恒为 false）
 * @param detail     人读判定依据（取了哪些事件、如何计算）
 */
public record MetricValue(
        MetricType metricType,
        boolean observed,
        Double value,
        double threshold,
        boolean breached,
        String detail
) {
}
