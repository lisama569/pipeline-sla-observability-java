package com.github.lisama569.pipelineslaobservabilityjava.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 从 application.properties 绑定流水线配置与评估周期。
 */
@ConfigurationProperties(prefix = "pipeline.observability")
public class PipelineObservabilityProperties {

    /** 后台周期性评估间隔（秒），<=0 表示关闭定时评估 */
    private long evaluationIntervalSeconds = 30;

    /** 每条流水线保留的历史评估结论条数（环形，超出淘汰，保证存储/查询开销有界） */
    private int evaluationHistoryLimit = 200;

    /** 每条流水线保留的已恢复告警条数 */
    private int recoveredAlertLimit = 100;

    private List<PipelineDef> pipelines = new ArrayList<>();

    public long getEvaluationIntervalSeconds() { return evaluationIntervalSeconds; }
    public void setEvaluationIntervalSeconds(long evaluationIntervalSeconds) {
        this.evaluationIntervalSeconds = evaluationIntervalSeconds;
    }
    public int getEvaluationHistoryLimit() { return evaluationHistoryLimit; }
    public void setEvaluationHistoryLimit(int evaluationHistoryLimit) {
        this.evaluationHistoryLimit = evaluationHistoryLimit;
    }
    public int getRecoveredAlertLimit() { return recoveredAlertLimit; }
    public void setRecoveredAlertLimit(int recoveredAlertLimit) {
        this.recoveredAlertLimit = recoveredAlertLimit;
    }
    public List<PipelineDef> getPipelines() { return pipelines; }
    public void setPipelines(List<PipelineDef> pipelines) { this.pipelines = pipelines; }

    /** 流水线定义（properties 绑定用可变形态）。 */
    public static class PipelineDef {
        private String id;
        private String displayName;
        private long windowSeconds = 600;
        private long retentionSeconds = 86400;
        private long futureToleranceSeconds = 60;
        private ThresholdsDef thresholds = new ThresholdsDef();

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getDisplayName() { return displayName; }
        public void setDisplayName(String displayName) { this.displayName = displayName; }
        public long getWindowSeconds() { return windowSeconds; }
        public void setWindowSeconds(long windowSeconds) { this.windowSeconds = windowSeconds; }
        public long getRetentionSeconds() { return retentionSeconds; }
        public void setRetentionSeconds(long retentionSeconds) { this.retentionSeconds = retentionSeconds; }
        public long getFutureToleranceSeconds() { return futureToleranceSeconds; }
        public void setFutureToleranceSeconds(long futureToleranceSeconds) { this.futureToleranceSeconds = futureToleranceSeconds; }
        public ThresholdsDef getThresholds() { return thresholds; }
        public void setThresholds(ThresholdsDef thresholds) { this.thresholds = thresholds; }
    }

    /** 阈值定义。 */
    public static class ThresholdsDef {
        private long maxFreshnessSeconds = 300;
        private long maxScheduleDelaySeconds = 120;
        private double maxFailureRatePercent = 0;
        private long escalationDelaySeconds = 600;
        private long recoveryDelaySeconds = 120;

        public long getMaxFreshnessSeconds() { return maxFreshnessSeconds; }
        public void setMaxFreshnessSeconds(long v) { this.maxFreshnessSeconds = v; }
        public long getMaxScheduleDelaySeconds() { return maxScheduleDelaySeconds; }
        public void setMaxScheduleDelaySeconds(long v) { this.maxScheduleDelaySeconds = v; }
        public double getMaxFailureRatePercent() { return maxFailureRatePercent; }
        public void setMaxFailureRatePercent(double v) { this.maxFailureRatePercent = v; }
        public long getEscalationDelaySeconds() { return escalationDelaySeconds; }
        public void setEscalationDelaySeconds(long v) { this.escalationDelaySeconds = v; }
        public long getRecoveryDelaySeconds() { return recoveryDelaySeconds; }
        public void setRecoveryDelaySeconds(long v) { this.recoveryDelaySeconds = v; }
    }
}
