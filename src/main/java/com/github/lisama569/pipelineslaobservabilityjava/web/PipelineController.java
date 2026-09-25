package com.github.lisama569.pipelineslaobservabilityjava.web;

import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.evaluation.EvaluationService;
import com.github.lisama569.pipelineslaobservabilityjava.evaluation.QueryService;
import java.time.Instant;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 流水线状态/评估查询接口（骨架）。 */
@RestController
@RequestMapping("/api/v1/pipelines")
public class PipelineController {

    private final QueryService queryService;
    private final EvaluationService evaluationService;

    public PipelineController(QueryService queryService, EvaluationService evaluationService) {
        this.queryService = queryService;
        this.evaluationService = evaluationService;
    }

    @GetMapping
    public List<PipelineConfig> list() {
        return queryService.listPipelines();
    }

    @GetMapping("/{pipelineId}/latest")
    public PipelineEvaluation latest(@PathVariable String pipelineId) {
        return queryService.latest(pipelineId);
    }

    @GetMapping("/{pipelineId}/history")
    public List<PipelineEvaluation> history(@PathVariable String pipelineId,
                                            @RequestParam(required = false) Integer limit) {
        return queryService.history(pipelineId, limit);
    }

    @PostMapping("/{pipelineId}/evaluate")
    public PipelineEvaluation evaluateNow(@PathVariable String pipelineId,
                                          @RequestParam(required = false) Instant asOf) {
        return asOf == null ? evaluationService.evaluate(pipelineId)
                            : evaluationService.evaluateAt(pipelineId, asOf);
    }

    @GetMapping("/alerts/active")
    public List<Alert> allActiveAlerts() {
        return queryService.allActiveAlerts();
    }
}
