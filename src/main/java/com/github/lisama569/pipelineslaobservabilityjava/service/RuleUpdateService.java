package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult;
import org.springframework.stereotype.Service;

/**
 * 在线规则更新：不重启即可修改单条流水线的评估规则。
 *
 * <p>语义约定：
 * <ul>
 *   <li>先整体校验，任一字段非法则整条拒绝（无任何字段生效，不允许只改一半），
 *       拒绝原因可区分（见 {@link RuleRejectReason}）。</li>
 *   <li>生效方式 = 基于当前生效规则构造全新规则对象并原子替换，
 *       并发读取要么看到旧规则、要么看到新规则，不会看到改了一半的规则。</li>
 *   <li>只影响后续评估与上报；已产生的告警快照不被追溯改写。</li>
 * </ul>
 */
@Service
public class RuleUpdateService {

    private final SlaProperties properties;
    private final Object lock = new Object();

    public RuleUpdateService(SlaProperties properties) {
        this.properties = properties;
    }

    /** 校验并应用一次部分更新（全量校验、整体生效）。 */
    public RuleUpdateResult update(String pipelineId, RuleUpdateRequest request) {
        synchronized (lock) {
            if (pipelineId == null || pipelineId.isBlank()) {
                return RuleUpdateResult.rejected(RuleRejectReason.MISSING_PIPELINE_ID,
                        "pipelineId 缺失");
            }
            if (request == null || request.isEmpty()) {
                return RuleUpdateResult.rejected(RuleRejectReason.NO_FIELDS,
                        "请求未携带任何待修改字段: pipelineId=" + pipelineId);
            }
            RuleUpdateResult invalid = validate(request);
            if (invalid != null) {
                return invalid;
            }
            // 以当前生效规则为基底，覆盖非空字段，构造全新对象后原子替换。
            SlaProperties.PipelineRule base = properties.ruleFor(pipelineId);
            SlaProperties.PipelineRule updated = new SlaProperties.PipelineRule();
            updated.setWindow(request.window() != null ? request.window() : base.getWindow());
            updated.setMaxFreshnessLag(request.maxFreshnessLag() != null
                    ? request.maxFreshnessLag() : base.getMaxFreshnessLag());
            updated.setMaxScheduleDelay(request.maxScheduleDelay() != null
                    ? request.maxScheduleDelay() : base.getMaxScheduleDelay());
            updated.setMinSlaAttainment(request.minSlaAttainment() != null
                    ? request.minSlaAttainment() : base.getMinSlaAttainment());
            updated.setRecoveryMargin(request.recoveryMargin() != null
                    ? request.recoveryMargin() : base.getRecoveryMargin());
            updated.setEscalateAfter(request.escalateAfter() != null
                    ? request.escalateAfter() : base.getEscalateAfter());
            properties.getPipelines().put(pipelineId, updated);
            return RuleUpdateResult.applied("规则已更新: pipelineId=" + pipelineId
                    + " window=" + updated.getWindow()
                    + " maxFreshnessLag=" + updated.getMaxFreshnessLag()
                    + " maxScheduleDelay=" + updated.getMaxScheduleDelay()
                    + " minSlaAttainment=" + updated.getMinSlaAttainment()
                    + " recoveryMargin=" + updated.getRecoveryMargin()
                    + " escalateAfter=" + updated.getEscalateAfter());
        }
    }

    /** 全量校验：任一字段非法即返回对应拒绝结果，全部合法返回 null。 */
    private RuleUpdateResult validate(RuleUpdateRequest r) {
        if (r.window() != null && (r.window().isZero() || r.window().isNegative())) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_WINDOW,
                    "评估窗口必须为正数: " + r.window());
        }
        if (r.maxFreshnessLag() != null && r.maxFreshnessLag().isNegative()) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_FRESHNESS_LAG,
                    "新鲜度阈值不能为负: " + r.maxFreshnessLag());
        }
        if (r.maxScheduleDelay() != null && r.maxScheduleDelay().isNegative()) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_SCHEDULE_DELAY,
                    "调度延迟阈值不能为负: " + r.maxScheduleDelay());
        }
        if (r.minSlaAttainment() != null && outsideUnitInterval(r.minSlaAttainment())) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_ATTAINMENT,
                    "达标率阈值必须在 [0,1]: " + r.minSlaAttainment());
        }
        if (r.recoveryMargin() != null && outsideUnitInterval(r.recoveryMargin())) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_RECOVERY_MARGIN,
                    "恢复裕量必须在 [0,1]: " + r.recoveryMargin());
        }
        if (r.escalateAfter() != null && r.escalateAfter() < 1) {
            return RuleUpdateResult.rejected(RuleRejectReason.INVALID_ESCALATE_AFTER,
                    "升级次数必须 >= 1: " + r.escalateAfter());
        }
        return null;
    }

    private static boolean outsideUnitInterval(double v) {
        return Double.isNaN(v) || v < 0.0 || v > 1.0;
    }

    /** 查询某流水线当前生效的规则快照。 */
    public SlaProperties.PipelineRule effectiveRule(String pipelineId) {
        return properties.ruleFor(pipelineId);
    }
}
