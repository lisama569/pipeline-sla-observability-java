package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RuleUpdateTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");

    private SlaProperties props;
    private EventStore store;
    private SlaEvaluator evaluator;
    private AlertManager alerts;

    @BeforeEach
    void setUp() {
        props = new SlaProperties();
        props.getDefaultRule().setMinSlaAttainment(0.9);
        props.getDefaultRule().setRecoveryMargin(0.05);
        store = new EventStore(props);
        evaluator = new SlaEvaluator(store, props);
        alerts = new AlertManager(props);
    }

    private void ingest(String pipeline, String run, RunStatus status, int minute) {
        var e = new JobRunEvent(pipeline, run, EventType.FINISHED, status,
                T0.plus(Duration.ofMinutes(minute)), T0.plus(Duration.ofMinutes(minute)));
        System.out.println("[RuleTest] 上报: " + e);
        store.ingest(e);
    }

    @Test
    void partialUpdateAppliesImmediatelyAndKeepsOtherFields() {
        var req = new RuleUpdateRequest(null, null, null, 0.5, null, null);
        var result = props.updateRule("p1", req);
        System.out.println("[RuleTest] 部分更新 -> " + result);
        assertEquals(RuleUpdateStatus.APPLIED, result.status());
        assertEquals(0.5, props.ruleFor("p1").getMinSlaAttainment(), "达标率阈值应立即生效");
        assertEquals(Duration.ofHours(1), props.ruleFor("p1").getWindow(), "未指定字段保持原值");
        assertEquals(3, props.ruleFor("p1").getEscalateAfter());

        // 同一组事件：改前违约（0.75 < 0.9），改后不再违约（0.75 >= 0.5）
        ingest("p1", "ok1", RunStatus.SUCCESS, 0);
        ingest("p1", "ok2", RunStatus.SUCCESS, 1);
        ingest("p1", "ok3", RunStatus.SUCCESS, 2);
        ingest("p1", "bad1", RunStatus.FAILED, 3);
        PipelineAssessment after = evaluator.evaluate("p1");
        System.out.println("[RuleTest] 改后评估=" + after);
        assertEquals(0.75, after.slaAttainment());
        assertFalse(after.slaBreached(), "新阈值 0.5 下 0.75 不再违约，改完立即生效");

        // 其他流水线仍用默认规则（互不影响）
        var other = props.updateRule("p2", new RuleUpdateRequest(null, null, null, null, null, null));
        assertEquals(0.9, other.effectiveRule().getMinSlaAttainment(), "p2 应继承默认规则，不受 p1 改动影响");
    }

    @Test
    void invalidUpdateIsRejectedAtomicallyWithDistinguishableReasons() {
        var r1 = props.updateRule("p1", new RuleUpdateRequest(Duration.ZERO, null, null, null, null, null));
        System.out.println("[RuleTest] 零窗口 -> " + r1);
        assertEquals(RuleUpdateStatus.REJECTED, r1.status());
        assertEquals(RuleRejectReason.INVALID_WINDOW, r1.reason());

        var r2 = props.updateRule("p1", new RuleUpdateRequest(Duration.ofMinutes(-5), null, null, null, null, null));
        assertEquals(RuleRejectReason.INVALID_WINDOW, r2.reason(), "负窗口与零窗口同属 INVALID_WINDOW");

        var r3 = props.updateRule("p1", new RuleUpdateRequest(null, null, null, 1.5, null, null));
        System.out.println("[RuleTest] 达标率超界 -> " + r3);
        assertEquals(RuleRejectReason.INVALID_MIN_SLA_ATTAINMENT, r3.reason());

        var r4 = props.updateRule("p1", new RuleUpdateRequest(null, null, null, null, -0.1, null));
        assertEquals(RuleRejectReason.INVALID_RECOVERY_MARGIN, r4.reason());

        var r5 = props.updateRule("p1", new RuleUpdateRequest(null, null, null, null, null, 0));
        System.out.println("[RuleTest] 升级次数 0 -> " + r5);
        assertEquals(RuleRejectReason.INVALID_ESCALATE_AFTER, r5.reason());
        assertNotEquals(r3.reason(), r5.reason(), "不同字段的拒绝原因必须可区分");

        // 原子性：合法字段 + 非法字段混在一起 → 整条不生效，合法字段也不得落下
        var mixed = props.updateRule("p1",
                new RuleUpdateRequest(Duration.ofMinutes(30), null, null, 2.0, null, null));
        System.out.println("[RuleTest] 混合非法 -> " + mixed);
        assertEquals(RuleUpdateStatus.REJECTED, mixed.status());
        assertFalse(props.getPipelines().containsKey("p1"),
                "校验不过就整条不生效，窗口改动不得部分落盘");
        assertEquals(Duration.ofHours(1), props.ruleFor("p1").getWindow(), "规则必须保持修改前的值");
    }

    @Test
    void updateDoesNotRewriteExistingAlerts() {
        // 旧规则下产生告警
        ingest("p1", "bad1", RunStatus.FAILED, 0);
        PipelineAssessment before = evaluator.evaluate("p1");
        System.out.println("[RuleTest] 改前评估=" + before);
        assertTrue(before.slaBreached());
        alerts.onAssessment(before);
        Alert fired = alerts.activeAlerts().get(0);
        System.out.println("[RuleTest] 改前告警=" + fired);

        // 在线放宽阈值：同一份事件改后不再违约
        var update = props.updateRule("p1", new RuleUpdateRequest(null, null, null, 0.0, null, null));
        assertEquals(RuleUpdateStatus.APPLIED, update.status());
        PipelineAssessment after = evaluator.evaluate("p1");
        System.out.println("[RuleTest] 改后评估=" + after);
        assertFalse(after.slaBreached(), "同一份事件按新规则不再违约，结论稳定一致");

        // 改之前已产生的告警仍是历史快照：firedAt / 详情不被追溯改写
        Alert historical = alerts.allAlerts().stream()
                .filter(a -> a.alertId().equals(fired.alertId())).findFirst().orElseThrow();
        assertEquals(fired.firedAt(), historical.firedAt(), "历史告警的触发时间不得被改写");
        assertEquals(fired.detail(), historical.detail(), "历史告警的触发详情不得被改写");
    }

    @Test
    void sameEventsYieldStableConclusionAcrossRepeatedEvaluation() {
        props.updateRule("p1", new RuleUpdateRequest(null, null, null, 0.5, null, null));
        ingest("p1", "ok1", RunStatus.SUCCESS, 0);
        ingest("p1", "bad1", RunStatus.FAILED, 1);
        PipelineAssessment a1 = evaluator.evaluate("p1");
        PipelineAssessment a2 = evaluator.evaluate("p1");
        System.out.println("[RuleTest] 重复评估: " + a1 + " / " + a2);
        assertEquals(a1, a2, "同一份事件在未再变更规则时结论必须稳定一致");
    }
}
