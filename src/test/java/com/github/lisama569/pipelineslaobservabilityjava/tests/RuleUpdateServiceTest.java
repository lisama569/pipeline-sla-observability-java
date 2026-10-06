package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateRequest;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.RuleUpdateService;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RuleUpdateServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");

    private SlaProperties props;
    private RuleUpdateService service;

    @BeforeEach
    void setUp() {
        props = new SlaProperties();
        service = new RuleUpdateService(props);
    }

    private void log(String phase, RuleUpdateResult r) {
        System.out.println("[RuleUpdateTest] " + phase + " -> " + r);
    }

    @Test
    void partialUpdateAppliesImmediatelyAndKeepsOtherFields() {
        RuleUpdateResult r = service.update("orders",
                new RuleUpdateRequest(null, Duration.ofMinutes(5), null, 0.5, null, null));
        log("部分更新", r);
        assertEquals(RuleUpdateStatus.APPLIED, r.status());

        SlaProperties.PipelineRule rule = props.ruleFor("orders");
        assertEquals(Duration.ofMinutes(5), rule.getMaxFreshnessLag(), "新阈值应立即生效");
        assertEquals(0.5, rule.getMinSlaAttainment());
        // 未修改字段继承更新前的生效值（默认规则）
        assertEquals(Duration.ofHours(1), rule.getWindow());
        assertEquals(0.05, rule.getRecoveryMargin());
        assertEquals(3, rule.getEscalateAfter());

        // 默认规则与其它流水线不受影响
        assertEquals(Duration.ofMinutes(30), props.getDefaultRule().getMaxFreshnessLag());
        assertEquals(Duration.ofMinutes(30), props.ruleFor("other").getMaxFreshnessLag());
    }

    @Test
    void invalidUpdatesAreRejectedWithDistinctReasonsAndNothingApplies() {
        RuleUpdateResult badWindow = service.update("orders",
                new RuleUpdateRequest(Duration.ZERO, null, null, null, null, null));
        log("窗口为 0", badWindow);
        assertEquals(RuleUpdateStatus.REJECTED, badWindow.status());
        assertEquals(RuleRejectReason.INVALID_WINDOW, badWindow.reason());

        RuleUpdateResult negWindow = service.update("orders",
                new RuleUpdateRequest(Duration.ofMinutes(-5), null, null, null, null, null));
        assertEquals(RuleRejectReason.INVALID_WINDOW, negWindow.reason());

        RuleUpdateResult badAttainment = service.update("orders",
                new RuleUpdateRequest(null, null, null, 1.5, null, null));
        log("达标率 >1", badAttainment);
        assertEquals(RuleRejectReason.INVALID_ATTAINMENT, badAttainment.reason());

        RuleUpdateResult badMargin = service.update("orders",
                new RuleUpdateRequest(null, null, null, null, -0.1, null));
        log("恢复裕量 <0", badMargin);
        assertEquals(RuleRejectReason.INVALID_RECOVERY_MARGIN, badMargin.reason());

        RuleUpdateResult badEscalate = service.update("orders",
                new RuleUpdateRequest(null, null, null, null, null, 0));
        log("升级次数 <1", badEscalate);
        assertEquals(RuleRejectReason.INVALID_ESCALATE_AFTER, badEscalate.reason());

        RuleUpdateResult empty = service.update("orders",
                new RuleUpdateRequest(null, null, null, null, null, null));
        log("空请求", empty);
        assertEquals(RuleRejectReason.NO_FIELDS, empty.reason());

        RuleUpdateResult noPipeline = service.update(" ",
                new RuleUpdateRequest(Duration.ofMinutes(5), null, null, null, null, null));
        log("缺 pipelineId", noPipeline);
        assertEquals(RuleRejectReason.MISSING_PIPELINE_ID, noPipeline.reason());

        assertTrue(props.getPipelines().isEmpty(), "校验失败不得留下任何规则记录");
    }

    @Test
    void rejectedUpdateDoesNotPartiallyApply() {
        // 同一条请求里既有合法字段也有非法字段：整条拒绝，合法字段也不生效
        RuleUpdateResult mixed = service.update("orders",
                new RuleUpdateRequest(Duration.ofMinutes(10), null, null, 2.0, null, null));
        log("混合请求", mixed);
        assertEquals(RuleUpdateStatus.REJECTED, mixed.status());
        assertEquals(RuleRejectReason.INVALID_ATTAINMENT, mixed.reason());
        assertFalse(props.getPipelines().containsKey("orders"),
                "校验不过就整条不生效，不能只改一半");
    }

    @Test
    void updatedRuleChangesSubsequentAssessmentButNotExistingAlerts() {
        EventStore store = new EventStore(props);
        SlaEvaluator evaluator = new SlaEvaluator(store, props);
        AlertManager alerts = new AlertManager(props);

        // 一轮 FAILED：达标率 0 → 违约并触发告警
        JobRunEvent started = new JobRunEvent("orders", "r1", EventType.STARTED, null, T0, T0.plusSeconds(30));
        JobRunEvent failed = new JobRunEvent("orders", "r1", EventType.FINISHED, RunStatus.FAILED,
                T0, T0.plusSeconds(90));
        System.out.println("[RuleUpdateTest] 上报: " + started);
        System.out.println("[RuleUpdateTest] 上报: " + failed);
        store.ingest(started);
        store.ingest(failed);
        PipelineAssessment before = evaluator.evaluate("orders");
        System.out.println("[RuleUpdateTest] 改前评估=" + before);
        assertTrue(before.slaBreached());
        alerts.onAssessment(before);
        Alert fired = alerts.activeAlerts().get(0);
        System.out.println("[RuleUpdateTest] 改前告警=" + fired);
        assertEquals(AlertStatus.FIRING, fired.status());

        // 在线把达标率阈值降到 0：同一批事件的后续评估不再违约
        RuleUpdateResult r = service.update("orders",
                new RuleUpdateRequest(null, null, null, 0.0, null, null));
        log("降阈值", r);
        assertEquals(RuleUpdateStatus.APPLIED, r.status());

        PipelineAssessment after = evaluator.evaluate("orders");
        System.out.println("[RuleUpdateTest] 改后评估=" + after);
        assertEquals(0.0, after.slaAttainment(), "同一份事件算出的指标值不变");
        assertFalse(after.slaBreached(), "新阈值应立即作用于后续评估");

        // 已产生的告警不被追溯改写
        Alert snapshot = alerts.allAlerts().get(0);
        assertEquals(fired.alertId(), snapshot.alertId());
        assertEquals(AlertStatus.FIRING, snapshot.status(), "规则更新不得追溯改写已产生的告警");
        assertEquals(fired.firedAt(), snapshot.firedAt());
    }

    @Test
    void sameEventsYieldStableConclusionUnderSameRule() {
        EventStore store = new EventStore(props);
        SlaEvaluator evaluator = new SlaEvaluator(store, props);
        for (int i = 0; i < 5; i++) {
            Instant at = T0.plus(Duration.ofMinutes(i));
            store.ingest(new JobRunEvent("orders", "run-" + i, EventType.FINISHED,
                    i < 4 ? RunStatus.SUCCESS : RunStatus.FAILED, at, at.plusSeconds(10)));
        }
        PipelineAssessment first = evaluator.evaluate("orders");
        PipelineAssessment second = evaluator.evaluate("orders");
        System.out.println("[RuleUpdateTest] 重复评估1=" + first);
        System.out.println("[RuleUpdateTest] 重复评估2=" + second);
        assertEquals(first, second, "同一规则下同一份事件的结论必须稳定一致");
    }
}
