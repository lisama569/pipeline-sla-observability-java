package com.github.lisama569.pipelineslaobservabilityjava.model;

import java.time.Instant;
import java.util.List;

/**
 * 单条流水线的一次评估结论（不可变，可复现）。
 *
 * @param pipelineId        流水线标识
 * @param evaluatedAt       评估使用的事件时间水位线（非墙钟时间，保证可复现）
 * @param windowStart       评估窗口起点
 * @param freshnessBreached 新鲜度是否违约
 * @param freshnessLagSeconds 距最近一次成功完成的滞后秒数（无成功记录时为 null）
 * @param maxScheduleDelaySeconds 窗口内最大调度延迟秒数
 * @param scheduleDelayBreached 调度延迟是否违约
 * @param slaAttainment     窗口内 SLA 达标率（0..1，窗口内无完结运行时为 null）
 * @param slaBreached       SLA 达标率是否违约
 * @param contributingEventKeys 参与本次评估的事件键（用于追溯）
 */
public record PipelineAssessment(
        String pipelineId,
        Instant evaluatedAt,
        Instant windowStart,
        boolean freshnessBreached,
        Long freshnessLagSeconds,
        long maxScheduleDelaySeconds,
        boolean scheduleDelayBreached,
        Double slaAttainment,
        boolean slaBreached,
        List<String> contributingEventKeys) {
}
