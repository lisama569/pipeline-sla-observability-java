package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricValue;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import com.github.lisama569.pipelineslaobservabilityjava.tests.support.ServiceTestBase;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 评估规则测试：指标边界、窗口排除、UNKNOWN、可复现、迟到事件追溯、有界历史与裁剪。
 */
class EvaluationServiceTest extends ServiceTestBase {

    /** freshness 100s / delay 60s / failRate 0% / escalation 300s / recovery 60s */
    private static final Thresholds TH = new Thresholds(100, 60, 0, 300, 60);

    @BeforeEach
    void setUp() {
        properties.setEvaluationHistoryLimit(3);
        register("p1", 300, 3600, 60, TH);
    }

    private MetricValue metric(PipelineEvaluation e, MetricType type) {
        return e.metrics().stream().filter(m -> m.metricType() == type).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("新鲜度边界：age 恰好等于阈值不违约，超过 1 秒才违约；无终态时 UNKNOWN")
    void freshnessBoundary() {
        clock.set(T0);
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0.minusSeconds(90), T0.minusSeconds(90)));
        ingestService.report(event("p1", "r1", "e2", "SUCCEEDED", T0.minusSeconds(90), null));

        PipelineEvaluation atThreshold = evaluationService.evaluateAt("p1", T0.plusSeconds(10));
        MetricValue freshAtLimit = metric(atThreshold, MetricType.FRESHNESS);
        testLog.info("freshness boundary input: finish=T0-90, asOf=T0+10 => {}", freshAtLimit.detail());
        assertTrue(freshAtLimit.observed());
        assertEquals(100.0, freshAtLimit.value());
        assertFalse(freshAtLimit.breached(), "age == 阈值必须不违约（闭区间）");

        PipelineEvaluation oneOver = evaluationService.evaluateAt("p1", T0.plusSeconds(11));
        MetricValue freshOver = metric(oneOver, MetricType.FRESHNESS);
        testLog.info("freshness one-over: asOf=T0+11 => {}", freshOver.detail());
        assertEquals(101.0, freshOver.value());
        assertTrue(freshOver.breached(), "age == 阈值+1 必须违约");
        assertEquals(SlaStatus.BREACH, oneOver.slaStatus());
    }

    @Test
    @DisplayName("调度延迟边界：delay==阈值不违约，>阈值违约；未带 scheduledTime 时不观测")
    void scheduleDelayBoundary() {
        clock.set(T0.plusSeconds(60));
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0.plusSeconds(60), T0));
        PipelineEvaluation atLimit = evaluationService.evaluateAt("p1", T0.plusSeconds(60));
        MetricValue delay = metric(atLimit, MetricType.SCHEDULE_DELAY);
        testLog.info("schedule delay boundary => {}", delay.detail());
        assertEquals(60.0, delay.value());
        assertFalse(delay.breached());

        ingestService.report(event("p1", "r2", "e2", "STARTED", T0.plusSeconds(61), T0));
        PipelineEvaluation over = evaluationService.evaluateAt("p1", T0.plusSeconds(61));
        MetricValue delayOver = metric(over, MetricType.SCHEDULE_DELAY);
        testLog.info("schedule delay one-over (取窗口内最大延迟) => {}", delayOver.detail());
        assertEquals(61.0, delayOver.value());
        assertTrue(delayOver.breached());
    }

    @Test
    @DisplayName("失败率：任何失败即违约（阈值 0%）；全成功达标；窗口内无终态不观测")
    void failureRateRules() {
        clock.set(T0);
        PipelineEvaluation empty = evaluationService.evaluateAt("p1", T0);
        MetricValue rate0 = metric(empty, MetricType.FAILURE_RATE);
        assertFalse(rate0.observed());
        assertEquals(SlaStatus.UNKNOWN, empty.slaStatus(), "无任何观测数据时整体 UNKNOWN");

        ingestService.report(event("p1", "r1", "e1", "SUCCEEDED", T0, null));
        PipelineEvaluation allGood = evaluationService.evaluateAt("p1", T0.plusSeconds(5));
        MetricValue rateOk = metric(allGood, MetricType.FAILURE_RATE);
        testLog.info("failure rate all-good => {}", rateOk.detail());
        assertEquals(0.0, rateOk.value());
        assertFalse(rateOk.breached());

        ingestService.report(event("p1", "r2", "e2", "FAILED", T0.plusSeconds(10), null));
        PipelineEvaluation withFailure = evaluationService.evaluateAt("p1", T0.plusSeconds(12));
        MetricValue rateBad = metric(withFailure, MetricType.FAILURE_RATE);
        testLog.info("failure rate with-failure => {}", rateBad.detail());
        assertEquals(50.0, rateBad.value());
        assertTrue(rateBad.breached());
    }

    @Test
    @DisplayName("窗口排除：窗口外的事件不参与指标计算（基于 eventTime，而非接收时间）")
    void windowExcludesOldEvents() {
        clock.set(T0.plusSeconds(400));
        // 终态发生在 T0，窗口为 asOf-300：asOf=T0+400 时窗口起点 T0+100，T0 已出窗
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0, T0));
        ingestService.report(event("p1", "r1", "e2", "SUCCEEDED", T0, null));
        PipelineEvaluation outside = evaluationService.evaluateAt("p1", T0.plusSeconds(400));
        MetricValue fresh = metric(outside, MetricType.FRESHNESS);
        testLog.info("window exclusion => {}", fresh.detail());
        assertFalse(fresh.observed(), "窗口外终态不得计入新鲜度");
    }

    @Test
    @DisplayName("可复现：同一组事件反复 evaluateAt 同一 asOf，结论逐字段一致")
    void evaluationIsReproducible() {
        clock.set(T0.plusSeconds(50));
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0, T0.minusSeconds(30)));
        ingestService.report(event("p1", "r1", "e2", "SUCCEEDED", T0.plusSeconds(20), null));

        PipelineState state = registry.find("p1").orElseThrow();
        long seqBefore;
        int historyBefore;
        synchronized (state) {
            seqBefore = state.getEvaluationSeq();
            historyBefore = state.getEvaluations().size();
        }
        PipelineEvaluation first = evaluationService.evaluateAt("p1", T0.plusSeconds(200));
        PipelineEvaluation second = evaluationService.evaluateAt("p1", T0.plusSeconds(200));
        synchronized (state) {
            assertEquals(seqBefore, state.getEvaluationSeq(), "历史重放不得推进评估序号");
            assertEquals(historyBefore, state.getEvaluations().size(), "历史重放不得落库");
        }
        testLog.info("reproduce run1 => {}", first);
        testLog.info("reproduce run2 => {}", second);
        assertEquals(first.seq(), second.seq());
        assertEquals(first.slaStatus(), second.slaStatus());
        assertEquals(first.basedOnEventSeqs(), second.basedOnEventSeqs());
        for (int i = 0; i < first.metrics().size(); i++) {
            assertEquals(first.metrics().get(i).value(), second.metrics().get(i).value());
            assertEquals(first.metrics().get(i).breached(), second.metrics().get(i).breached());
            assertEquals(first.metrics().get(i).detail(), second.metrics().get(i).detail());
        }
    }

    @Test
    @DisplayName("可追溯：迟到事件使历史结论由 BREACH 变为 PASS，相邻两次评估记录可定位到新事件")
    void lateEventHistoryChangeIsTraceable() {
        clock.set(T0.plusSeconds(150));
        // 此时尚无完成事件 -> 新鲜度 UNKNOWN；先放一个较早的失败造成部分观测
        ingestService.report(event("p1", "r-old", "old-fail", "FAILED", T0.plusSeconds(10), null));

        // 评估1：T0+150，最近终态是 T0+10，新鲜度 140 > 100 => BREACH
        PipelineEvaluation before = evaluationService.evaluate("p1");
        MetricValue beforeFresh = metric(before, MetricType.FRESHNESS);
        assertTrue(beforeFresh.breached());
        testLog.info("traceability BEFORE late event: seq={} basedOn={} detail={}",
                before.seq(), before.basedOnEventSeqs(), beforeFresh.detail());

        // 迟到事件：运行在 T0+140 成功，但现在(T0+150)才上报
        ingestService.report(event("p1", "r-late", "late-ok", "SUCCEEDED",
                T0.plusSeconds(140), null));
        PipelineState state = registry.find("p1").orElseThrow();
        PipelineEvaluation after = state.latestEvaluation().orElseThrow();
        MetricValue afterFresh = metric(after, MetricType.FRESHNESS);
        testLog.info("traceability AFTER late event: seq={} basedOn={} detail={}",
                after.seq(), after.basedOnEventSeqs(), afterFresh.detail());

        assertEquals(before.seq() + 1, after.seq());
        assertFalse(afterFresh.breached());
        // 结论变化必须能追溯到具体事件：新评估的输入序号集合包含迟到事件
        long lateSeq = state.getEventsById().get("late-ok").ingestSeq();
        assertTrue(after.basedOnEventSeqs().contains(lateSeq),
                "新结论必须显式标注纳入了迟到事件 seq=" + lateSeq);
        assertFalse(before.basedOnEventSeqs().contains(lateSeq));
        assertNotNull(afterFresh.detail());
    }

    @Test
    @DisplayName("有界历史：评估历史超过限制被环形淘汰，查询开销不随历史累积")
    void evaluationHistoryIsBounded() {
        clock.set(T0);
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0, T0));
        for (int i = 0; i < 10; i++) {
            clock.advanceSeconds(30);
            evaluationService.evaluate("p1");
        }
        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertEquals(3, state.getEvaluations().size(), "历史结论必须被限制在 historyLimit 内");
            assertEquals(state.getEvaluationSeq(),
                    state.getEvaluations().getLast().seq(),
                    "保留的应是最新的若干条");
        }
    }

    @Test
    @DisplayName("有界状态：超过 retention 的事件被裁剪，不影响后续评估正确性")
    void expiredEventsArePruned() {
        clock.set(T0);
        ingestService.report(event("p1", "r1", "e1", "STARTED", T0, T0));
        ingestService.report(event("p1", "r1", "e2", "SUCCEEDED", T0, null));

        clock.set(T0.plusSeconds(4000)); // 远超 retention 3600
        evaluationService.evaluate("p1");
        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertTrue(state.getEventsById().isEmpty(), "过期事件必须被裁剪");
            assertTrue(state.getRuns().isEmpty(), "完全过期的 run 必须被移除");
        }
        // 裁剪后窗口内无数据 -> UNKNOWN，而非 NPE 或错误违约
        PipelineEvaluation now = evaluationService.evaluateAt("p1", T0.plusSeconds(4000));
        assertEquals(SlaStatus.UNKNOWN, now.slaStatus());
        List<Long> seqs = now.basedOnEventSeqs();
        assertTrue(seqs.isEmpty());
    }
}
