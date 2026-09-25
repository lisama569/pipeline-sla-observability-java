package com.github.lisama569.pipelineslaobservabilityjava.web;

import com.github.lisama569.pipelineslaobservabilityjava.alert.AlertService;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.evaluation.QueryService;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 告警生命周期查询与手动恢复。 */
@RestController
@RequestMapping("/api/v1/pipelines/{pipelineId}/alerts")
public class AlertController {

    private final QueryService queryService;
    private final PipelineRegistry registry;
    private final AlertService alertService;

    public AlertController(QueryService queryService, PipelineRegistry registry,
                           AlertService alertService) {
        this.queryService = queryService;
        this.registry = registry;
        this.alertService = alertService;
    }

    @GetMapping("/active")
    public List<Alert> active(@PathVariable String pipelineId) {
        return queryService.activeAlerts(pipelineId);
    }

    @GetMapping("/recovered")
    public List<Alert> recovered(@PathVariable String pipelineId) {
        return queryService.recoveredAlerts(pipelineId);
    }

    @PostMapping("/{alertId}/recover")
    public Alert manualRecover(@PathVariable String pipelineId, @PathVariable String alertId) {
        PipelineState state = registry.find(pipelineId)
                .orElseThrow(() -> new IllegalArgumentException("pipeline not found: " + pipelineId));
        synchronized (state) {
            return alertService.manualRecover(state, alertId);
        }
    }
}
