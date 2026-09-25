package com.github.lisama569.pipelineslaobservabilityjava.controller;

import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 对外 REST 接口：上报与查询。
 *
 * <p>上报被接受后立即对该流水线做一次评估并推进告警生命周期；
 * 重复/被拒绝的上报不触发任何状态变化。查询接口为只读。
 */
@RestController
@RequestMapping("/api")
public class ObservabilityController {

    private final EventStore eventStore;
    private final SlaEvaluator evaluator;
    private final AlertManager alertManager;

    public ObservabilityController(EventStore eventStore, SlaEvaluator evaluator, AlertManager alertManager) {
        this.eventStore = eventStore;
        this.evaluator = evaluator;
        this.alertManager = alertManager;
    }

    @PostMapping("/events")
    public ResponseEntity<IngestResult> ingest(@RequestBody JobRunEvent event) {
        IngestResult result = eventStore.ingest(event);
        if (result.status() == IngestStatus.ACCEPTED) {
            PipelineAssessment assessment = evaluator.evaluate(event.pipelineId());
            alertManager.onAssessment(assessment);
            return ResponseEntity.ok(result);
        }
        if (result.status() == IngestStatus.DUPLICATE) {
            return ResponseEntity.ok(result); // 幂等：重复上报视为成功但无副作用
        }
        return ResponseEntity.unprocessableEntity().body(result);
    }

    @GetMapping("/pipelines/{pipelineId}/assessment")
    public PipelineAssessment assess(@PathVariable String pipelineId) {
        return evaluator.evaluate(pipelineId);
    }

    @GetMapping("/pipelines/assessments")
    public List<PipelineAssessment> assessAll() {
        return evaluator.evaluateAll();
    }

    @GetMapping("/alerts")
    public List<Alert> alerts(@RequestParam(defaultValue = "false") boolean includeResolved) {
        return includeResolved ? alertManager.allAlerts() : alertManager.activeAlerts();
    }
}
