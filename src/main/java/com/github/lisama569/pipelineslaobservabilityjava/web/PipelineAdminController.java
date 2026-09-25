package com.github.lisama569.pipelineslaobservabilityjava.web;

import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineObservabilityProperties;
import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineConfigRegistrar;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 运行时注册流水线（骨架），便于本地动态验证。 */
@RestController
@RequestMapping("/api/v1/admin/pipelines")
public class PipelineAdminController {

    private final PipelineRegistry registry;

    public PipelineAdminController(PipelineRegistry registry) {
        this.registry = registry;
    }

    @PostMapping
    public PipelineConfig register(@RequestBody PipelineObservabilityProperties.PipelineDef def) {
        PipelineConfig config = PipelineConfigRegistrar.toDomain(def);
        registry.register(config);
        return config;
    }
}
