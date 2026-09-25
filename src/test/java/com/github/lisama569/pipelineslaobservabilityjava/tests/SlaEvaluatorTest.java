package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SlaEvaluatorTest {

    private static final Instant T0 = Instant.parse("2026-09-26T00:00:00Z");

    private SlaProperties props;
    private EventStore store;
    private SlaEvaluator evaluator;

    @BeforeEach
    void setUp() {
        props = new SlaProperties();
        SlaProperties.PipelineRule rule = props.getDefaultRule();
        rule.setWindow(Duration.ofHours(1));
        rule.setMaxFreshnessLag(Duration.ofMinutes(30));
        rule.setMaxScheduleDelay(Duration.ofMinutes(10));
        rule.setMinSlaAttainment(0.9);
        props.setRetention(Duration.ofHours(2));
        store = new EventStore(props);
        evaluator = new SlaEvaluator(store, props);
    }

    private JobRunEvent started(String run, Instant scheduled, Instant occurred) {
        return new JobRunEvent("p1", run, EventType.STARTED, null, scheduled, occurred);
    }

    private JobRunEvent finished(String run, RunStatus status, Instant scheduled, Instant occurred) {
        return new JobRunEvent("p1", run, EventType.FINISHED, status, scheduled, occurred);
    }

    private void log(PipelineAssessment a) {
        System.out.println("[SlaEvaluatorTest] 评估: pipeline=" + a.pipelineId()
                + " 水位线=" + a.evaluatedAt() + " 窗口起点=" + a.windowStart()
                + " freshnessLag=" + a.freshnessLagSeconds() + "s breached=" + a.freshnessBreached()
                + " maxDelay=" + a.maxScheduleDelaySeconds() + "s breached=" + a.scheduleDelayBreached()
                + " attainment=" + a.slaAttainment() + " breached=" + a.slaBreached()
                + " 参与事件=" + a.contributingEventKeys());
    }

    /** 构造一组固定事件：3 次运行，2 成功 1 失败，最大调度延迟 15 分钟。 */
    private List<JobRunEvent> fixture() {
        List<JobRunEvent> events = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Instant scheduled = T0.plus(Duration.ofMinutes(10L * i));
            Instant start = scheduled.plus(Duration.ofMinutes(5L * (i + 1))); // 5/10/15 分钟延迟
            Instant finish = start.plus(Duration.ofMinutes(2));
            events.add(started("run-" + i, scheduled, start));
            events.add(finished("run-" + i, i == 2 ? RunStatus.FAILED : RunStatus.SUCCESS, scheduled, finish));
        }
        return events;
    }

    @Test
    void sameEventsInDifferentOrderProduceIdenticalAssessment() {
        List<JobRunEvent> events = fixture();
        events.forEach(store::ingest);
        PipelineAssessment inOrder = evaluator.evaluate("p1");
        log(inOrder);

        EventStore shuffledStore = new EventStore(props);
        SlaEvaluator shuffledEvaluator = new SlaEvaluator(shuffledStore, props);
        List<JobRunEvent> shuffled = fixture();
        Collections.shuffle(shuffled, new java.util.Random(42));
        shuffled.forEach(e -> {
            var r = shuffledStore.ingest(e);
            System.out.println("[SlaEvaluatorTest] 乱序上报 " + e.eventType() + " " + e.runId()
                    + " -> " + r.status());
        });
        PipelineAssessment outOfOrder = shuffledEvaluator.evaluate("p1");
        log(outOfOrder);

        assertEquals(inOrder, outOfOrder, "同一组事件无论到达顺序如何，评估结论必须一致");
    }

    @Test
    void thresholdBoundariesAreExact() {
        fixture().forEach(store::ingest);
        PipelineAssessment a = evaluator.evaluate("p1");
        log(a);

        // 水位线 = 最后一个 FINISHED = T0+37min；最近成功 = run-1 完成于 T0+22min → 滞后 15min < 30min
        assertFalse(a.freshnessBreached(), "新鲜度滞后 15min 未超 30min 阈值");
        assertEquals(900L, a.freshnessLagSeconds());

        // 最大调度延迟 15min > 10min 阈值
        assertTrue(a.scheduleDelayBreached(), "最大调度延迟 15min 超过 10min 阈值");
        assertEquals(900L, a.maxScheduleDelaySeconds());

        // 达标率 2/3 ≈ 0.667 < 0.9
        assertTrue(a.slaBreached(), "达标率 2/3 低于 0.9 阈值");
        assertEquals(2.0 / 3.0, a.slaAttainment(), 1e-9);
    }

    @Test
    void freshnessBoundaryIsInclusiveOfThreshold() {
        // 成功完成时间距水位线恰好 30min（=阈值）→ 不违约；30min+1s → 违约
        store.ingest(finished("r1", RunStatus.SUCCESS, T0, T0));
        store.ingest(started("r2", T0.plus(Duration.ofMinutes(30)), T0.plus(Duration.ofMinutes(30))));
        PipelineAssessment atThreshold = evaluator.evaluate("p1");
        log(atThreshold);
        assertEquals(1800L, atThreshold.freshnessLagSeconds());
        assertFalse(atThreshold.freshnessBreached(), "滞后恰好等于阈值不应违约");

        EventStore store2 = new EventStore(props);
        SlaEvaluator evaluator2 = new SlaEvaluator(store2, props);
        store2.ingest(finished("r1", RunStatus.SUCCESS, T0, T0));
        store2.ingest(started("r2", T0.plus(Duration.ofMinutes(30)).plusSeconds(1),
                T0.plus(Duration.ofMinutes(30)).plusSeconds(1)));
        PipelineAssessment beyond = evaluator2.evaluate("p1");
        log(beyond);
        assertEquals(1801L, beyond.freshnessLagSeconds());
        assertTrue(beyond.freshnessBreached(), "滞后超过阈值 1 秒即违约");
    }

    @Test
    void lateEventChangesAssessmentAndIsTraceable() {
        fixture().forEach(store::ingest);
        PipelineAssessment before = evaluator.evaluate("p1");
        log(before);
        assertEquals(2.0 / 3.0, before.slaAttainment(), 1e-9);

        // 迟到事件：窗口内补一条 FAILED，结论变化且可追溯
        JobRunEvent late = finished("run-late", RunStatus.FAILED, T0, T0.plus(Duration.ofMinutes(25)));
        var ingestResult = store.ingest(late);
        System.out.println("[SlaEvaluatorTest] 迟到事件 " + late + " -> " + ingestResult.status());
        PipelineAssessment after = evaluator.evaluate("p1");
        log(after);

        assertEquals(2.0 / 4.0, after.slaAttainment(), 1e-9, "迟到的 FAILED 应拉低达标率");
        assertTrue(after.contributingEventKeys().contains("p1|run-late|FINISHED"),
                "结论变化必须可追溯到具体事件键");
        assertNotEquals(before.slaAttainment(), after.slaAttainment());
    }
}
