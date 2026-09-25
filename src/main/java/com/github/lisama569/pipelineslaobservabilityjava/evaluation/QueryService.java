package com.github.lisama569.pipelineslaobservabilityjava.evaluation;

import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineRegistry;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * 只读查询。所有读取在流水线锁内拷贝快照，避免与并发上报/评估共享可变结构。
 */
@Service
public class QueryService {

    private final PipelineRegistry registry;

    public QueryService(PipelineRegistry registry) {
        this.registry = registry;
    }

    public List<PipelineConfig> listPipelines() {
        return registry.all().stream().map(PipelineState::getConfig).toList();
    }

    public PipelineEvaluation latest(String pipelineId) {
        PipelineState state = require(pipelineId);
        synchronized (state) {
            return state.latestEvaluation().orElse(null);
        }
    }

    /** 评估历史（最新在前），limit 为空时返回全部（已受 historyLimit 环形淘汰约束）。 */
    public List<PipelineEvaluation> history(String pipelineId, Integer limit) {
        PipelineState state = require(pipelineId);
        synchronized (state) {
            List<PipelineEvaluation> all = new ArrayList<>(state.evaluationList());
            java.util.Collections.reverse(all);
            if (limit != null && limit >= 0 && limit < all.size()) {
                return all.subList(0, limit);
            }
            return all;
        }
    }

    public List<Alert> activeAlerts(String pipelineId) {
        PipelineState state = require(pipelineId);
        synchronized (state) {
            return List.copyOf(state.getActiveAlerts().values());
        }
    }

    /** 已恢复告警历史（最新在前）。 */
    public List<Alert> recoveredAlerts(String pipelineId) {
        PipelineState state = require(pipelineId);
        synchronized (state) {
            List<Alert> all = new ArrayList<>(state.getRecoveredAlerts());
            java.util.Collections.reverse(all);
            return all;
        }
    }

    public List<Alert> allActiveAlerts() {
        List<Alert> alerts = new ArrayList<>();
        for (PipelineState state : registry.all()) {
            synchronized (state) {
                alerts.addAll(state.getActiveAlerts().values());
            }
        }
        return List.copyOf(alerts);
    }

    private PipelineState require(String pipelineId) {
        return registry.find(pipelineId).orElseThrow(() -> new ResponseStatusException(
                HttpStatus.NOT_FOUND, "pipeline not found: " + pipelineId));
    }
}
