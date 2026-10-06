package com.github.lisama569.pipelineslaobservabilityjava.controller;

import com.github.lisama569.pipelineslaobservabilityjava.model.AckRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService;
import com.github.lisama569.pipelineslaobservabilityjava.service.RuleUpdateService;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
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
    private final MaintenanceService maintenanceService;
    private final RuleUpdateService ruleUpdateService;

    public ObservabilityController(EventStore eventStore, SlaEvaluator evaluator, AlertManager alertManager,
                                   MaintenanceService maintenanceService, RuleUpdateService ruleUpdateService) {
        this.eventStore = eventStore;
        this.evaluator = evaluator;
        this.alertManager = alertManager;
        this.maintenanceService = maintenanceService;
        this.ruleUpdateService = ruleUpdateService;
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

    /** 声明维护窗口（幂等：相同窗口重复声明返回 DUPLICATE，无副作用）。 */
    @PostMapping("/maintenance")
    public ResponseEntity<MaintenanceResult> declareMaintenance(@RequestBody MaintenanceRequest request) {
        MaintenanceResult result = maintenanceService.declare(
                request.pipelineId(), request.metricType(), request.start(), request.end(), request.note());
        if (result.status() == MaintenanceStatus.REJECTED) {
            return ResponseEntity.unprocessableEntity().body(result);
        }
        return ResponseEntity.ok(result);
    }

    /** 撤销维护窗口（按幂等键定位；不存在返回 404）。 */
    @PostMapping("/maintenance/revoke")
    public ResponseEntity<MaintenanceResult> revokeMaintenance(@RequestBody MaintenanceRequest request) {
        MaintenanceResult result = maintenanceService.revoke(
                request.pipelineId(), request.metricType(), request.start(), request.end());
        if (result.status() == MaintenanceStatus.REJECTED) {
            return ResponseEntity.unprocessableEntity().body(result);
        }
        if (result.status() == MaintenanceStatus.NOT_FOUND) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    /** 查询当前生效的维护窗口。 */
    @GetMapping("/maintenance")
    public List<MaintenanceWindow> maintenance(@RequestParam(required = false) String pipelineId) {
        return maintenanceService.list(pipelineId);
    }

    /**
     * 认领一条活动中的告警。重复认领幂等（200 + DUPLICATE）；
     * 已恢复 / 不存在 / 缺认领人返回 422，原因可区分。
     */
    @PostMapping("/alerts/{alertId}/ack")
    public ResponseEntity<AckResult> ack(@PathVariable String alertId, @RequestBody AckRequest request) {
        // 认领时间取事件时间水位线（无事件时退化为墙钟），保证重放一致。
        Instant at = eventStore.watermark().orElseGet(Instant::now);
        AckResult result = alertManager.ack(alertId, request.ackBy(), request.note(), at);
        if (result.status() == AckStatus.REJECTED) {
            return ResponseEntity.unprocessableEntity().body(result);
        }
        return ResponseEntity.ok(result);
    }

    /** 在线更新单条流水线的评估规则（全量校验、整体生效）。 */
    @PutMapping("/rules/{pipelineId}")
    public ResponseEntity<RuleUpdateResult> updateRule(@PathVariable String pipelineId,
                                                       @RequestBody RuleUpdateRequest request) {
        RuleUpdateResult result = ruleUpdateService.update(pipelineId, request);
        if (result.status() == RuleUpdateStatus.REJECTED) {
            return ResponseEntity.unprocessableEntity().body(result);
        }
        return ResponseEntity.ok(result);
    }

    /** 查询某流水线当前生效的规则。 */
    @GetMapping("/rules/{pipelineId}")
    public Object effectiveRule(@PathVariable String pipelineId) {
        return ruleUpdateService.effectiveRule(pipelineId);
    }
}
