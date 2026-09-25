package com.github.lisama569.pipelineslaobservabilityjava.tests.support;

import com.github.lisama569.pipelineslaobservabilityjava.alert.AlertService;
import com.github.lisama569.pipelineslaobservabilityjava.config.PipelineObservabilityProperties;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.evaluation.EvaluationService;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.EventReport;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.IngestService;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 手工装配全部服务（不启动 Spring），时钟可控、状态隔离。 */
public abstract class ServiceTestBase {

    protected static final Instant T0 = Instant.parse("2026-09-26T00:00:00Z");

    protected final Logger testLog = LoggerFactory.getLogger(getClass());
    protected MutableClock clock = new MutableClock(T0);
    protected PipelineObservabilityProperties properties = new PipelineObservabilityProperties();
    protected PipelineRegistry registry = new PipelineRegistry();
    protected AlertService alertService = new AlertService(clock, properties);
    protected EvaluationService evaluationService =
            new EvaluationService(registry, clock, alertService, properties);
    protected IngestService ingestService =
            new IngestService(registry, clock, evaluationService);

    protected PipelineConfig register(String id, long windowSeconds, long retentionSeconds,
                                      long futureToleranceSeconds, Thresholds thresholds) {
        PipelineConfig config = new PipelineConfig(id, id, windowSeconds, retentionSeconds,
                futureToleranceSeconds, thresholds);
        registry.register(config);
        return config;
    }

    /** 断言并把“输入事件 + 判定依据”打到单测日志。 */
    protected IngestResult logDecision(String caseName, EventReport input, IngestResult result) {
        testLog.info("[{}] INPUT  => pipeline={}, run={}, event={}, type={}, eventTime={}, scheduled={}",
                caseName, input == null ? null : input.pipelineId(),
                input == null ? null : input.runId(),
                input == null ? null : input.eventId(),
                input == null ? null : input.eventType(),
                input == null ? null : input.eventTime(),
                input == null ? null : input.scheduledTime());
        testLog.info("[{}] DECIDE => outcome={}, reason={}, seq={}, message={}",
                caseName, result.outcome(), result.reason(), result.ingestSeq(), result.message());
        return result;
    }

    protected EventReport event(String pipeline, String run, String eventId, String type,
                                Instant eventTime, Instant scheduledTime) {
        return new EventReport(pipeline, run, eventId, type, eventTime, scheduledTime);
    }
}
