package com.github.lisama569.pipelineslaobservabilityjava.config;

import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 启动时把 application.properties 中配置的流水线注册进注册表。
 */
@Component
public class PipelineBootstrapInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(PipelineBootstrapInitializer.class);

    private final PipelineObservabilityProperties properties;
    private final PipelineRegistry registry;

    public PipelineBootstrapInitializer(PipelineObservabilityProperties properties,
                                        PipelineRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (PipelineObservabilityProperties.PipelineDef def : properties.getPipelines()) {
            PipelineConfig config = PipelineConfigRegistrar.toDomain(def);
            registry.register(config);
            log.info("registered pipeline from config: {} (window={}s, retention={}s)",
                    config.pipelineId(), config.windowSeconds(), config.retentionSeconds());
        }
        log.info("pipeline bootstrap done, {} pipeline(s) registered", properties.getPipelines().size());
    }
}
