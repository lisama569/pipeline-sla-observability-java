package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 流水线可观测配置：评估窗口、迟到保留范围、时钟容差与阈值。
 *
 * @param pipelineId            流水线标识
 * @param displayName           展示名称
 * @param windowSeconds         评估窗口长度
 * @param retentionSeconds      迟到事件可追溯范围：eventTime 早于 now-retention 拒绝为 EXPIRED
 * @param futureToleranceSeconds eventTime 允许领先服务端时钟的最大秒数（时钟偏差容差）
 * @param thresholds            阈值配置
 */
public record PipelineConfig(
        String pipelineId,
        String displayName,
        long windowSeconds,
        long retentionSeconds,
        long futureToleranceSeconds,
        Thresholds thresholds
) {
}
