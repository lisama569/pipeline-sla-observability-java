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
    private final com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService maintenanceService;
    private final com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties properties;

    public ObservabilityController(EventStore eventStore, SlaEvaluator evaluator, AlertManager alertManager,
                                   com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService maintenanceService,
                                   com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties properties) {
        this.eventStore = eventStore;
        this.evaluator = evaluator;
        this.alertManager = alertManager;
        this.maintenanceService = maintenanceService;
        this.properties = properties;
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

    /** 认领一条活动告警。 */
    @PostMapping("/alerts/{alertId}/ack")
    public ResponseEntity<com.github.lisama569.pipelineslaobservabilityjava.model.AckResult> ack(
            @PathVariable String alertId,
            @RequestBody com.github.lisama569.pipelineslaobservabilityjava.model.AckRequest request) {
        if (request == null || request.operator() == null || request.operator().isBlank()) {
            return ResponseEntity.badRequest().body(
                    new com.github.lisama569.pipelineslaobservabilityjava.model.AckResult(
                            com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus.INVALID_REQUEST,
                            null, "认领人 operator 不能为空"));
        }
        var result = alertManager.acknowledge(alertId, request.operator(), request.note(),
                request.acknowledgedAt() != null ? request.acknowledgedAt() : java.time.Instant.now());
        return switch (result.status()) {
            case ACKNOWLEDGED, ALREADY_ACKNOWLEDGED -> ResponseEntity.ok(result);
            case ALERT_NOT_FOUND -> ResponseEntity.status(404).body(result);
            case ALERT_ALREADY_RESOLVED -> ResponseEntity.unprocessableEntity().body(result);
            case INVALID_REQUEST -> ResponseEntity.badRequest().body(result);
        };
    }

    /** 声明维护 / 静默窗口（幂等）。 */
    @PostMapping("/maintenance")
    public ResponseEntity<com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult> declareMaintenance(
            @RequestBody com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow window) {
        var result = maintenanceService.declare(window);
        return switch (result.status()) {
            case DECLARED -> ResponseEntity.ok(result);
            case DUPLICATE -> ResponseEntity.ok(result); // 幂等：重复声明视为成功但无副作用
            case REJECTED -> ResponseEntity.unprocessableEntity().body(result);
            default -> ResponseEntity.unprocessableEntity().body(result);
        };
    }

    /** 撤销维护窗口。 */
    @DeleteMapping("/maintenance/{windowId}")
    public ResponseEntity<com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult> revokeMaintenance(
            @PathVariable String windowId) {
        var result = maintenanceService.revoke(windowId);
        return switch (result.status()) {
            case REVOKED -> ResponseEntity.ok(result);
            case NOT_FOUND -> ResponseEntity.status(404).body(result);
            default -> ResponseEntity.unprocessableEntity().body(result);
        };
    }

    /** 查询当前已登记的维护窗口。 */
    @GetMapping("/maintenance")
    public List<com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow> maintenanceWindows() {
        return maintenanceService.list();
    }

    /** 在线更新某条流水线的评估规则（全量前置校验，失败则整条不生效）。 */
    @PutMapping("/rules/{pipelineId}")
    public ResponseEntity<com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult> updateRule(
            @PathVariable String pipelineId,
            @RequestBody com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest request) {
        var result = properties.updateRule(pipelineId, request);
        return switch (result.status()) {
            case APPLIED -> ResponseEntity.ok(result);
            case REJECTED -> ResponseEntity.unprocessableEntity().body(result);
        };
    }

    /** 查询某条流水线当前生效的规则。 */
    @GetMapping("/rules/{pipelineId}")
    public com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties.PipelineRule rule(
            @PathVariable String pipelineId) {
        return properties.ruleFor(pipelineId);
    }
}
