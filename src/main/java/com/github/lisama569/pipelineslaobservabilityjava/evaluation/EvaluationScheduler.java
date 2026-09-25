package com.github.lisama569.pipelineslaobservabilityjava.evaluation;

import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineObservabilityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 周期性触发全量流水线评估。
 * 间隔可配；评估本身在每条流水线各自的锁内执行，与并发上报互不丢失更新。
 */
@Component
public class EvaluationScheduler {

    private static final Logger log = LoggerFactory.getLogger(EvaluationScheduler.class);

    private final EvaluationService evaluationService;
    private final PipelineObservabilityProperties properties;

    public EvaluationScheduler(EvaluationService evaluationService,
                               PipelineObservabilityProperties properties) {
        this.evaluationService = evaluationService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "#{${pipeline.observability.evaluation-interval-seconds:30} * 1000L}")
    public void tick() {
        if (properties.getEvaluationIntervalSeconds() <= 0) {
            return;
        }
        int count = evaluationService.evaluateAll().size();
        log.debug("scheduled evaluation finished for {} pipeline(s)", count);
    }
}
