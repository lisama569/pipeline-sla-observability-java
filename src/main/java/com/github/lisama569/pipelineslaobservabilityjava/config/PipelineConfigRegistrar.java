package com.github.lisama569.pipelineslaobservabilityjava.config;

import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 启用配置绑定并把配置中的流水线注册到注册表（骨架：空实现）。
 */
@Configuration
@EnableConfigurationProperties(PipelineObservabilityProperties.class)
public class PipelineConfigRegistrar {

    private final PipelineObservabilityProperties properties;
    private final PipelineRegistry registry;

    public PipelineConfigRegistrar(PipelineObservabilityProperties properties, PipelineRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    /** 把可变配置定义转换成不可变领域配置。 */
    public static PipelineConfig toDomain(PipelineObservabilityProperties.PipelineDef def) {
        var t = def.getThresholds();
        return new PipelineConfig(
                def.getId(),
                def.getDisplayName(),
                def.getWindowSeconds(),
                def.getRetentionSeconds(),
                def.getFutureToleranceSeconds(),
                new Thresholds(
                        t.getMaxFreshnessSeconds(),
                        t.getMaxScheduleDelaySeconds(),
                        t.getMaxFailureRatePercent(),
                        t.getEscalationDelaySeconds(),
                        t.getRecoveryDelaySeconds()));
    }
}
