package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestOutcome;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.domain.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.domain.SlaStatus;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.EventReport;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import com.github.lisama569.pipelineslaobservabilityjava.tests.support.ServiceTestBase;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 上报语义测试：重复上报、乱序迟到、时钟回拨、矛盾拒绝、边界窗口。
 */
class IngestServiceTest extends ServiceTestBase {

    private static final Thresholds TH = new Thresholds(
            300, 120, 0, 600, 120);

    @BeforeEach
    void setUp() {
        register("p1", 600, 3600, 60, TH);
    }

    @Test
    @DisplayName("正常生命周期：STARTED 与 SUCCEEDED 被接收，事件落库且触发评估")
    void acceptsStartedAndSucceeded() {
        IngestResult r1 = logDecision("happy-started",
                event("p1", "run-1", "e1", "STARTED", T0, T0.minusSeconds(10)),
                ingestService.report(event("p1", "run-1", "e1", "STARTED", T0, T0.minusSeconds(10))));
        assertEquals(IngestOutcome.ACCEPTED, r1.outcome());
        assertEquals(1, r1.ingestSeq());

        clock.advanceSeconds(60);
        IngestResult r2 = logDecision("happy-succeeded",
                event("p1", "run-1", "e2", "SUCCEEDED", T0.plusSeconds(30), null),
                ingestService.report(event("p1", "run-1", "e2", "SUCCEEDED", T0.plusSeconds(30), null)));
        assertEquals(IngestOutcome.ACCEPTED, r2.outcome());
        assertEquals(2, r2.ingestSeq());

        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertEquals(2, state.getEventsById().size());
            assertEquals(1, state.getRuns().size());
            PipelineEvaluation latest = state.latestEvaluation().orElseThrow();
            assertSame(SlaStatus.PASS, latest.slaStatus());
        }
    }

    @Test
    @DisplayName("幂等：同一 eventId 完全相同的重复上报返回 DUPLICATE 且序号不变、不重复计数")
    void duplicateReportIsIdempotent() {
        EventReport report = event("p1", "run-1", "e-dup", "STARTED", T0, T0);
        IngestResult first = logDecision("dup-first", report, ingestService.report(report));
        IngestResult second = logDecision("dup-second", report, ingestService.report(report));
        clock.advanceSeconds(120);
        IngestResult third = logDecision("dup-third-late", report, ingestService.report(report));

        assertEquals(IngestOutcome.ACCEPTED, first.outcome());
        assertEquals(IngestOutcome.DUPLICATE, second.outcome());
        assertEquals(IngestOutcome.DUPLICATE, third.outcome());
        // 重复上报必须返回首次接收时的序号
        assertEquals(first.ingestSeq(), second.ingestSeq());
        assertEquals(first.ingestSeq(), third.ingestSeq());

        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertEquals(1, state.getEventsById().size(), "重复上报不得重复计数");
            assertEquals(1, state.getOrderedEvents().size());
            assertEquals(1, state.getRuns().size());
            // 重复上报不重新评估：历史评估只有首次上报产生的一条
            assertEquals(1, state.getEvaluations().size(), "重复上报不得触发新评估/改变结论");
        }
    }

    @Test
    @DisplayName("矛盾：同一 eventId 携带不同载荷 -> CONFLICT，且不产生任何写入")
    void sameEventIdDifferentPayloadRejected() {
        EventReport first = event("p1", "run-1", "e-x", "STARTED", T0, T0);
        EventReport conflicting = event("p1", "run-1", "e-x", "SUCCEEDED", T0.plusSeconds(5), null);
        assertEquals(IngestOutcome.ACCEPTED,
                logDecision("payload-first", first, ingestService.report(first)).outcome());
        IngestResult rejected = logDecision("payload-conflict", conflicting,
                ingestService.report(conflicting));
        assertEquals(IngestOutcome.REJECTED, rejected.outcome());
        assertEquals(RejectReason.CONFLICT, rejected.reason());

        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            assertEquals(1, state.getEventsById().size(), "矛盾拒绝必须不部分生效");
            JobRunEvent stored = state.getEventsById().get("e-x");
            assertEquals("STARTED", stored.eventType().name());
        }
    }

    @Test
    @DisplayName("矛盾：重复 STARTED、重复/异种终态、起止时间倒挂均被拒绝为 CONFLICT")
    void lifecycleConflictsRejected() {
        // 先建立 run-1：迟到顺序：终态先到、STARTED 后到（乱序）
        IngestResult terminalFirst = logDecision("oo-succeeded-first",
                event("p1", "run-1", "t1", "SUCCEEDED", T0.plusSeconds(20), null),
                ingestService.report(event("p1", "run-1", "t1", "SUCCEEDED", T0.plusSeconds(20), null)));
        assertEquals(IngestOutcome.ACCEPTED, terminalFirst.outcome());

        IngestResult startedLate = logDecision("oo-started-late",
                event("p1", "run-1", "s1", "STARTED", T0, T0),
                ingestService.report(event("p1", "run-1", "s1", "STARTED", T0, T0)));
        assertEquals(IngestOutcome.ACCEPTED, startedLate.outcome(),
                "保留窗口内的迟到 STARTED 必须照常接收");

        IngestResult secondStarted = logDecision("dup-started",
                event("p1", "run-1", "s2", "STARTED", T0.plusSeconds(1), T0),
                ingestService.report(event("p1", "run-1", "s2", "STARTED", T0.plusSeconds(1), T0)));
        assertEquals(RejectReason.CONFLICT, secondStarted.reason());

        IngestResult secondTerminalSameType = logDecision("dup-terminal",
                event("p1", "run-1", "t2", "SUCCEEDED", T0.plusSeconds(21), null),
                ingestService.report(event("p1", "run-1", "t2", "SUCCEEDED", T0.plusSeconds(21), null)));
        assertEquals(RejectReason.CONFLICT, secondTerminalSameType.reason());

        IngestResult otherTerminal = logDecision("other-terminal",
                event("p1", "run-1", "t3", "FAILED", T0.plusSeconds(22), null),
                ingestService.report(event("p1", "run-1", "t3", "FAILED", T0.plusSeconds(22), null)));
        assertEquals(RejectReason.CONFLICT, otherTerminal.reason());

        // 另一个 run：STARTED 在 T0+100，终态 T0+99 早于开始时间 -> 倒挂 CONFLICT
        clock.set(T0.plusSeconds(120));
        assertEquals(IngestOutcome.ACCEPTED, ingestService.report(
                event("p1", "run-2", "s3", "STARTED", T0.plusSeconds(100), T0.plusSeconds(100))).outcome());
        IngestResult inverted = logDecision("inverted-finish",
                event("p1", "run-2", "t4", "FAILED", T0.plusSeconds(99), null),
                ingestService.report(event("p1", "run-2", "t4", "FAILED", T0.plusSeconds(99), null)));
        assertEquals(RejectReason.CONFLICT, inverted.reason());
    }

    @Test
    @DisplayName("迟到事件：超出 retention 拒绝为 EXPIRED；边界上恰好等于 cutoff 必须接收")
    void expirationBoundary() {
        // clock=T0+3600, retention=3600：eventTime=T0 恰好不早于 cutoff(T0)，接收
        clock.set(T0.plusSeconds(3600));
        EventReport atBoundary = event("p1", "run-b", "e-bound", "STARTED", T0, T0);
        IngestResult boundaryResult = logDecision("expire-boundary-equal", atBoundary,
                ingestService.report(atBoundary));
        assertEquals(IngestOutcome.ACCEPTED, boundaryResult.outcome(),
                "eventTime == now-retention 应接收（边界闭区间）");

        EventReport oneSecondTooOld = event("p1", "run-b", "e-old", "STARTED",
                T0.minusSeconds(1), T0.minusSeconds(1));
        IngestResult expired = logDecision("expire-one-second-too-old", oneSecondTooOld,
                ingestService.report(oneSecondTooOld));
        assertEquals(IngestOutcome.REJECTED, expired.outcome());
        assertEquals(RejectReason.EXPIRED, expired.reason());
        assertNotNull(expired.message());
    }

    @Test
    @DisplayName("时钟偏差：eventTime 领先超过容差拒绝为 FUTURE_EVENT_TIME；容差边界接收")
    void futureEventTimeBoundary() {
        EventReport atTolerance = event("p1", "run-f", "e-f1", "STARTED",
                T0.plusSeconds(60), T0);
        IngestResult boundary = logDecision("future-equal-tolerance", atTolerance,
                ingestService.report(atTolerance));
        assertEquals(IngestOutcome.ACCEPTED, boundary.outcome(),
                "eventTime == now+tolerance 应接收（边界闭区间）");

        EventReport beyondTolerance = event("p1", "run-f", "e-f2", "STARTED",
                T0.plusSeconds(61), T0);
        IngestResult rejected = logDecision("future-beyond-tolerance", beyondTolerance,
                ingestService.report(beyondTolerance));
        assertEquals(RejectReason.FUTURE_EVENT_TIME, rejected.reason());
    }

    @Test
    @DisplayName("时钟回拨：回拨后接收更早 eventTime 的事件，ingestSeq 仍单调、排序仍确定")
    void clockRewoundEventsStayOrdered() {
        // 1) 服务端时钟在 T0 时先收到一个 eventTime=T0+30 的事件（未来容差内）
        IngestResult later = logDecision("rewind-later-event-first",
                event("p1", "run-r", "e-late", "STARTED", T0.plusSeconds(30), T0.plusSeconds(30)),
                ingestService.report(event("p1", "run-r", "e-late", "STARTED",
                        T0.plusSeconds(30), T0.plusSeconds(30))));
        assertEquals(IngestOutcome.ACCEPTED, later.outcome());

        // 2) 服务端时钟回拨 200s 后恢复到 T0+10：eventTime=T0 的事件迟到到达
        clock.rewindSeconds(200);
        clock.set(T0.plusSeconds(10));
        IngestResult earlier = logDecision("rewind-earlier-event",
                event("p1", "run-r2", "e-early", "STARTED", T0, T0),
                ingestService.report(event("p1", "run-r2", "e-early", "STARTED", T0, T0)));
        assertEquals(IngestOutcome.ACCEPTED, earlier.outcome());
        // 全局接收序号严格递增，不受墙上时钟回拨影响
        assertEquals(later.ingestSeq() + 1, earlier.ingestSeq());

        PipelineState state = registry.find("p1").orElseThrow();
        synchronized (state) {
            List<JobRunEvent> ordered = List.copyOf(state.getOrderedEvents().values());
            assertEquals(T0, ordered.get(0).eventTime(), "排序按 eventTime，乱序/回拨不影响全序");
            assertEquals(T0.plusSeconds(30), ordered.get(1).eventTime());
        }
    }

    @Test
    @DisplayName("无法归属/格式错误：未注册流水线、缺字段、非法 eventType 均有可区分拒绝原因")
    void unregisteredAndMalformedHaveDistinctReasons() {
        IngestResult unregistered = logDecision("unregistered",
                event("nope", "r", "e", "STARTED", T0, T0),
                ingestService.report(event("nope", "r", "e", "STARTED", T0, T0)));
        assertEquals(RejectReason.UNREGISTERED_PIPELINE, unregistered.reason());

        IngestResult missingRun = logDecision("missing-run",
                event("p1", " ", "e", "STARTED", T0, T0),
                ingestService.report(event("p1", " ", "e", "STARTED", T0, T0)));
        assertEquals(RejectReason.MALFORMED, missingRun.reason());

        IngestResult missingTime = logDecision("missing-time",
                event("p1", "r", "e", "STARTED", null, null),
                ingestService.report(event("p1", "r", "e", "STARTED", null, null)));
        assertEquals(RejectReason.MALFORMED, missingTime.reason());

        IngestResult badType = logDecision("bad-type",
                event("p1", "r", "e", "CANCELLED", T0, null),
                ingestService.report(event("p1", "r", "e", "CANCELLED", T0, null)));
        assertEquals(RejectReason.MALFORMED, badType.reason());

        IngestResult nullBody = ingestService.report(null);
        assertEquals(RejectReason.MALFORMED, nullBody.reason());
    }

    @Test
    @DisplayName("乱序上报的可复现性：先到终态后到 STARTED，与顺序到达在同一 asOf 得到相同结论")
    void outOfOrderArrivalProducesSameConclusion() {
        Instant asOf = T0.plusSeconds(90);
        // 顺序 A：STARTED -> SUCCEEDED
        ingestService.report(event("p1", "runA", "a-s", "STARTED", T0, T0));
        clock.advanceSeconds(60);
        ingestService.report(event("p1", "runA", "a-t", "SUCCEEDED", T0.plusSeconds(30), null));

        // 顺序 B：SUCCEEDED 先到、STARTED 迟到
        register("p2", 600, 3600, 60, TH);
        ingestService.report(event("p2", "runB", "b-t", "SUCCEEDED", T0.plusSeconds(30), null));
        clock.advanceSeconds(60);
        ingestService.report(event("p2", "runB", "b-s", "STARTED", T0, T0));

        // 在同一个生效时刻重放评估，隔离到达时间差异
        PipelineEvaluation evalA = evaluationService.evaluateAt("p1", asOf);
        PipelineEvaluation evalB = evaluationService.evaluateAt("p2", asOf);

        testLog.info("repro A asOf={} metrics={}", evalA.asOf(), evalA.metrics());
        testLog.info("repro B asOf={} metrics={}", evalB.asOf(), evalB.metrics());
        assertEquals(SlaStatus.PASS, evalA.slaStatus());
        assertEquals(evalA.slaStatus(), evalB.slaStatus());
        assertEquals(evalA.metrics().size(), evalB.metrics().size());
        for (int i = 0; i < evalA.metrics().size(); i++) {
            assertEquals(evalA.metrics().get(i).metricType(), evalB.metrics().get(i).metricType());
            assertEquals(evalA.metrics().get(i).observed(), evalB.metrics().get(i).observed());
            assertEquals(evalA.metrics().get(i).value(), evalB.metrics().get(i).value(),
                    "同一组事件乱序到达，指标取值必须一致");
            assertEquals(evalA.metrics().get(i).breached(), evalB.metrics().get(i).breached());
        }
    }
}
