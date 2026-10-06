package com.github.lisama569.pipelineslaobservabilityjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * SLA 评估配置（可通过 application.properties 覆盖）。
 */
@Component
@ConfigurationProperties(prefix = "sla")
public class SlaProperties {

    public static class PipelineRule {
        /** 评估窗口长度。 */
        private Duration window = Duration.ofHours(1);
        /** 新鲜度阈值：距最近一次成功完成允许的最大滞后。 */
        private Duration maxFreshnessLag = Duration.ofMinutes(30);
        /** 调度延迟阈值：started - scheduled 允许的最大值。 */
        private Duration maxScheduleDelay = Duration.ofMinutes(10);
        /** SLA 达标率阈值（低于该值视为违约）。 */
        private double minSlaAttainment = 0.9;
        /** 恢复滞回：达标率需恢复到 阈值+recoveryMargin 才算恢复，避免阈值抖动。 */
        private double recoveryMargin = 0.05;
        /** 连续违约评估次数达到该值后告警升级为 CRITICAL。 */
        private int escalateAfter = 3;

        public Duration getWindow() { return window; }
        public void setWindow(Duration window) { this.window = window; }
        public Duration getMaxFreshnessLag() { return maxFreshnessLag; }
        public void setMaxFreshnessLag(Duration maxFreshnessLag) { this.maxFreshnessLag = maxFreshnessLag; }
        public Duration getMaxScheduleDelay() { return maxScheduleDelay; }
        public void setMaxScheduleDelay(Duration maxScheduleDelay) { this.maxScheduleDelay = maxScheduleDelay; }
        public double getMinSlaAttainment() { return minSlaAttainment; }
        public void setMinSlaAttainment(double minSlaAttainment) { this.minSlaAttainment = minSlaAttainment; }
        public double getRecoveryMargin() { return recoveryMargin; }
        public void setRecoveryMargin(double recoveryMargin) { this.recoveryMargin = recoveryMargin; }
        public int getEscalateAfter() { return escalateAfter; }
        public void setEscalateAfter(int escalateAfter) { this.escalateAfter = escalateAfter; }
    }

    /** 事件保留窗口：早于 (水位线 - retention) 的事件被清理，新上报被拒绝。 */
    private Duration retention = Duration.ofHours(2);

    /** 为 true 时，未在 pipelines 中显式注册的流水线上报将被拒绝。 */
    private boolean requireRegisteredPipelines = false;

    /** 默认规则（未单独配置的流水线使用）。 */
    private PipelineRule defaultRule = new PipelineRule();

    /** 按流水线的覆盖规则（并发读写安全；在线更新整体替换条目，不在原地改字段）。 */
    private final Map<String, PipelineRule> pipelines = new java.util.concurrent.ConcurrentHashMap<>();

    public PipelineRule getDefaultRule() { return defaultRule; }
    public void setDefaultRule(PipelineRule defaultRule) { this.defaultRule = defaultRule; }
    public Map<String, PipelineRule> getPipelines() { return pipelines; }
    public void setPipelines(Map<String, PipelineRule> pipelines) {
        this.pipelines.clear();
        if (pipelines != null) {
            this.pipelines.putAll(pipelines);
        }
    }
    public Duration getRetention() { return retention; }
    public void setRetention(Duration retention) { this.retention = retention; }
    public boolean isRequireRegisteredPipelines() { return requireRegisteredPipelines; }
    public void setRequireRegisteredPipelines(boolean requireRegisteredPipelines) { this.requireRegisteredPipelines = requireRegisteredPipelines; }

    public PipelineRule ruleFor(String pipelineId) {
        return pipelines.getOrDefault(pipelineId, defaultRule);
    }

    /**
     * 在线更新某条流水线的规则：先对全部入参做整体校验，任一字段非法则整条不生效；
     * 校验通过后构造新的 PipelineRule 原子替换（不在原地改字段，读取方不会看到改了一半的规则）。
     * 已产生的告警为历史快照，不被追溯改写。
     */
    public synchronized com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult updateRule(
            String pipelineId, com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest request) {
        if (pipelineId == null || pipelineId.isBlank()) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_PIPELINE_ID,
                    "pipelineId 为空");
        }
        if (request == null) {
            request = new com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest(
                    null, null, null, null, null, null);
        }
        // 全量前置校验：任一非法即整体拒绝，原因可区分。
        if (request.window() != null && (request.window().isZero() || request.window().isNegative())) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_WINDOW,
                    "评估窗口必须为正数: " + request.window());
        }
        if (request.maxFreshnessLag() != null && request.maxFreshnessLag().isNegative()) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_MAX_FRESHNESS_LAG,
                    "新鲜度阈值不得为负: " + request.maxFreshnessLag());
        }
        if (request.maxScheduleDelay() != null && request.maxScheduleDelay().isNegative()) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_MAX_SCHEDULE_DELAY,
                    "调度延迟阈值不得为负: " + request.maxScheduleDelay());
        }
        if (request.minSlaAttainment() != null
                && (request.minSlaAttainment().isNaN()
                || request.minSlaAttainment() < 0.0 || request.minSlaAttainment() > 1.0)) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_MIN_SLA_ATTAINMENT,
                    "达标率阈值必须落在 [0,1]: " + request.minSlaAttainment());
        }
        if (request.recoveryMargin() != null
                && (request.recoveryMargin().isNaN()
                || request.recoveryMargin() < 0.0 || request.recoveryMargin() > 1.0)) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_RECOVERY_MARGIN,
                    "恢复裕量必须落在 [0,1]: " + request.recoveryMargin());
        }
        if (request.escalateAfter() != null && request.escalateAfter() < 1) {
            return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.rejected(
                    com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason.INVALID_ESCALATE_AFTER,
                    "升级次数必须 >= 1: " + request.escalateAfter());
        }

        PipelineRule base = ruleFor(pipelineId);
        PipelineRule updated = new PipelineRule();
        updated.setWindow(request.window() != null ? request.window() : base.getWindow());
        updated.setMaxFreshnessLag(request.maxFreshnessLag() != null ? request.maxFreshnessLag() : base.getMaxFreshnessLag());
        updated.setMaxScheduleDelay(request.maxScheduleDelay() != null ? request.maxScheduleDelay() : base.getMaxScheduleDelay());
        updated.setMinSlaAttainment(request.minSlaAttainment() != null ? request.minSlaAttainment() : base.getMinSlaAttainment());
        updated.setRecoveryMargin(request.recoveryMargin() != null ? request.recoveryMargin() : base.getRecoveryMargin());
        updated.setEscalateAfter(request.escalateAfter() != null ? request.escalateAfter() : base.getEscalateAfter());
        pipelines.put(pipelineId, updated);
        return com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult.applied(
                updated, "规则已在线更新: " + pipelineId);
    }

    /** 所有规则中最长的评估窗口（用于计算有效保留期）。 */
    public Duration maxWindow() {
        Duration max = defaultRule.getWindow();
        for (PipelineRule rule : pipelines.values()) {
            if (rule.getWindow().compareTo(max) > 0) {
                max = rule.getWindow();
            }
        }
        return max;
    }
}
