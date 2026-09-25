package com.github.lisama569.pipelineslaobservabilityjava.domain;

import java.time.Instant;
import java.util.List;

/**
 * 一条流水线在某个评估时刻的完整评估结论。
 *
 * @param pipelineId  流水线
 * @param seq         该流水线评估序号（单调递增，可复现/可追溯的定位锚点）
 * @param asOf        评估生效时刻（决定纳入哪些 eventTime 的事件）
 * @param windowStart 评估窗口起点（asOf - windowSeconds）
 * @param metrics     各指标取值与判定
 * @param slaStatus   汇总 SLA 状态
 * @param basedOnEventSeqs 本次计算纳入的事件 ingestSeq 列表（结论可追溯到具体事件）
 * @param evaluatedAt 实际执行评估的墙上时间
 */
public record PipelineEvaluation(
        String pipelineId,
        long seq,
        Instant asOf,
        Instant windowStart,
        List<MetricValue> metrics,
        SlaStatus slaStatus,
        List<Long> basedOnEventSeqs,
        Instant evaluatedAt
) {
}
