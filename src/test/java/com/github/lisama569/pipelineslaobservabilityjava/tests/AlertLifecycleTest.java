package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.AlertState;
import com.github.lisama569.pipelineslaobservabilityjava.domain.AlertTransition;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import com.github.lisama569.pipelineslaobservabilityjava.tests.support.ServiceTestBase;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警生命周期测试：触发去重、阈值抖动、升级、迟滞自动恢复、恢复后再违约开新生命周期。
 */
class AlertLifecycleTest extends ServiceTestBase {

    /** freshness 100s 触发；升级 300s；恢复需连续 60s 正常 */
    private static final Thresholds TH = new Thresholds(100, 10_000, 100, 300, 60);

    @BeforeEach
    void setUp() {
        register("p1", 3600, 7200, 3600, TH);
        clock.set(T0);
    }

    private Alert active(MetricType type) {
        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            return state.getActiveAlerts().get(type);
        }
    }

    private void finish(String run, Instant at) {
        ingestService.report(event("p1", run, "e-" + run + "-" + at, "SUCCEEDED", at, null));
    }

    @Test
    @DisplayName("阈值抖动：违约/达标反复切换只保留一个 FIRING 告警，不重复触发")
    void flappingDoesNotDuplicateAlerts() {
        // 完成发生在 T0-150，T0 时新鲜度 150 > 100 => 违约，触发告警
        finish("r0", T0.minusSeconds(150));
        Alert a1 = active(MetricType.FRESHNESS);
        testLog.info("alert fired: {}", a1.getId());
        assertEquals(AlertState.FIRING, a1.getState());

        // 抖动1：T0+20 来一次刚完成 => 新鲜度 20，达标但距离上次违约仅 20s < 60s，保持 FIRING
        clock.set(T0.plusSeconds(20));
        finish("r1", T0.plusSeconds(20));
        Alert stillFiring = active(MetricType.FRESHNESS);
        assertEquals(a1.getId(), stillFiring.getId(), "短暂恢复不得关闭/重建告警");
        assertEquals(AlertState.FIRING, stillFiring.getState());

        // 抖动2：T0+40 又变老（新运行在 T0-150 之后没有更新鲜的完成……构造新违约）
        // 通过让“最近完成”回到更早：上一次完成是 T0+20，无法回退；因此直接推进时间到 T0+200，
        // 期间无新完成 => 新鲜度 180 再次违约，只刷新 lastBreachAt
        clock.set(T0.plusSeconds(200));
        evaluationService.evaluate("p1");
        Alert flappedAgain = active(MetricType.FRESHNESS);
        assertEquals(a1.getId(), flappedAgain.getId(), "再次违约必须复用同一告警，不能重复触发");

        // 整个抖动期间迁移记录只有一次 TRIGGERED
        long triggered = a1.getTransitions().stream()
                .filter(t -> t.reason().equals("TRIGGERED")).count();
        assertEquals(1, triggered, "抖动不得产生重复 TRIGGERED 迁移");
        assertEquals(0, registry.find("p1").orElseThrow().getRecoveredAlerts().size());
    }

    @Test
    @DisplayName("升级：FIRING 持续违约达到 escalationDelay 后升级为 ESCALATED，且只升级一次")
    void escalationHappensOnceAfterDuration() {
        finish("r0", T0.minusSeconds(150));
        Alert alert = active(MetricType.FRESHNESS);
        String id = alert.getId();

        clock.set(T0.plusSeconds(300));
        evaluationService.evaluate("p1");
        Alert at300 = active(MetricType.FRESHNESS);
        assertEquals(id, at300.getId());
        assertEquals(AlertState.ESCALATED, at300.getState(), "持续违约 300s 应升级");
        testLog.info("escalated transitions: {}", at300.getTransitions());

        // 再评估一次，不能重复升级
        clock.set(T0.plusSeconds(320));
        evaluationService.evaluate("p1");
        Alert stillOne = active(MetricType.FRESHNESS);
        long escalations = stillOne.getTransitions().stream()
                .filter(t -> t.reason().equals("ESCALATED_BY_DURATION")).count();
        assertEquals(1, escalations, "升级只能发生一次");
        assertEquals(AlertState.ESCALATED, stillOne.getState());
    }

    @Test
    @DisplayName("自动恢复：达标持续时间不足 recoveryDelay 不恢复；满足后自动 RECOVERED")
    void recoveryRequiresSustainedNormalPeriod() {
        finish("r0", T0.minusSeconds(150));
        Alert fired = active(MetricType.FRESHNESS);

        // T0+10 恢复达标，但仅维持 30s 后再次违约
        clock.set(T0.plusSeconds(10));
        finish("r1", T0.plusSeconds(10));
        assertEquals(AlertState.FIRING, active(MetricType.FRESHNESS).getState());
        clock.set(T0.plusSeconds(40));
        evaluationService.evaluate("p1"); // 仍有 r1 完成，age=30，达标，距上次违约 30s
        assertEquals(AlertState.FIRING, active(MetricType.FRESHNESS).getState(),
                "恢复观察期不足不得恢复");

        // 等够恢复观察期
        clock.set(T0.plusSeconds(80));
        evaluationService.evaluate("p1"); // age=70，距上次违约(T0-150 之后最近违约评估)...
        // 最近违约评估发生在 T0(触发时 asOf=T0)；lastBreachAt 在抖动违约刷新——本例无抖动，
        // lastBreachAt=T0(触发)。T0+80 达标且 asOf-lastBreachAt=80 >= 60 => 恢复
        assertFalse(registry.find("p1").orElseThrow().getActiveAlerts()
                .containsKey(MetricType.FRESHNESS), "满足观察期后必须自动恢复");
        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertEquals(1, state.getRecoveredAlerts().size());
            Alert recovered = state.getRecoveredAlerts().getLast();
            assertEquals(fired.getId(), recovered.getId());
            assertEquals(AlertState.RECOVERED, recovered.getState());
            assertEquals(T0.plusSeconds(80), recovered.getRecoveredAt());
            testLog.info("recovered transitions: {}", recovered.getTransitions());
        }
    }

    @Test
    @DisplayName("恢复后再次违约形成全新告警生命周期，旧告警保留迁移历史")
    void newLifecycleAfterRecovery() {
        finish("r0", T0.minusSeconds(150));
        Alert first = active(MetricType.FRESHNESS);

        // 持续达标完成恢复
        clock.set(T0.plusSeconds(10));
        finish("r1", T0.plusSeconds(10));
        clock.set(T0.plusSeconds(80));
        evaluationService.evaluate("p1");
        assertEquals(AlertState.RECOVERED, registry.find("p1").orElseThrow()
                .getRecoveredAlerts().getLast().getState());

        // 之后流水线再次停摆：推进到 T0+400，新鲜度再次 > 100
        clock.set(T0.plusSeconds(400));
        evaluationService.evaluate("p1");
        Alert second = active(MetricType.FRESHNESS);
        testLog.info("first={} second={}", first.getId(), second.getId());
        assertNotEquals(first.getId(), second.getId(), "再次违约必须开启新生命周期");
        assertEquals(AlertState.FIRING, second.getState());
        assertEquals(1, second.getTransitions().size());
        AlertTransition trigger = second.getTransitions().get(0);
        assertEquals(AlertState.FIRING, trigger.to());

        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            // 同时存在 1 个已恢复 + 1 个活跃，各自独立
            assertEquals(1, state.getRecoveredAlerts().size());
            assertEquals(1, state.getActiveAlerts().size());
            assertEquals(AlertState.RECOVERED, state.getRecoveredAlerts().getLast().getState());
        }
    }

    @Test
    @DisplayName("状态不倒退：已恢复告警不会被后续操作改写状态；手动恢复留痕")
    void recoveredAlertNeverRegresses() {
        finish("r0", T0.minusSeconds(150));
        Alert a = active(MetricType.FRESHNESS);
        // 手动恢复
        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            alertService.manualRecover(state, a.getId());
            assertEquals(AlertState.RECOVERED, a.getState());
            // 再次手动恢复必须被状态机挡住（抛异常或无效果），这里验证实体状态不回退
        }
        boolean threw = false;
        try {
            synchronized (state) {
                alertService.manualRecover(state, a.getId());
            }
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw, "已恢复告警不在活跃集合中，再次恢复应明确报错而非状态倒退");
        long manual = a.getTransitions().stream()
                .filter(t -> t.reason().equals("MANUAL_RECOVERED")).count();
        assertEquals(1, manual);
        // 之后再违约是新告警
        clock.set(T0.plusSeconds(300));
        evaluationService.evaluate("p1");
        assertNotEquals(a.getId(), active(MetricType.FRESHNESS).getId());
    }

    @Test
    @DisplayName("无观测数据不得触发或恢复告警")
    void unobservedMetricDoesNotRaiseAlert() {
        PipelineEvaluation empty = evaluationService.evaluateAt("p1", T0);
        assertTrue(empty.metrics().stream().noneMatch(m -> m.observed() && m.breached()));
        assertEquals(0, registry.find("p1").orElseThrow().getActiveAlerts().size());
        assertEquals(0, registry.find("p1").orElseThrow().getRecoveredAlerts().size());
    }
}
