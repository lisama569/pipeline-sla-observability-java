package com.github.lisama569.pipelineslaobservabilityjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    /** 按流水线的覆盖规则（并发容器，支持在线更新与并发读取）。 */
    private Map<String, PipelineRule> pipelines = new ConcurrentHashMap<>();

    public PipelineRule getDefaultRule() { return defaultRule; }
    public void setDefaultRule(PipelineRule defaultRule) { this.defaultRule = defaultRule; }
    public Map<String, PipelineRule> getPipelines() { return pipelines; }
    public void setPipelines(Map<String, PipelineRule> pipelines) {
        this.pipelines = new ConcurrentHashMap<>(pipelines);
    }
    public Duration getRetention() { return retention; }
    public void setRetention(Duration retention) { this.retention = retention; }
    public boolean isRequireRegisteredPipelines() { return requireRegisteredPipelines; }
    public void setRequireRegisteredPipelines(boolean requireRegisteredPipelines) { this.requireRegisteredPipelines = requireRegisteredPipelines; }

    public PipelineRule ruleFor(String pipelineId) {
        return pipelines.getOrDefault(pipelineId, defaultRule);
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
