package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyTest {

    private static final Instant T0 = Instant.parse("2026-09-26T00:00:00Z");
    private static final int RUNS = 400;

    private SlaProperties props() {
        SlaProperties props = new SlaProperties();
        SlaProperties.PipelineRule rule = props.getDefaultRule();
        rule.setWindow(Duration.ofHours(1));
        rule.setMaxFreshnessLag(Duration.ofMinutes(30));
        rule.setMaxScheduleDelay(Duration.ofMinutes(10));
        rule.setMinSlaAttainment(0.9);
        props.setRetention(Duration.ofHours(2));
        return props;
    }

    private List<JobRunEvent> fixture() {
        List<JobRunEvent> events = new ArrayList<>();
        for (int i = 0; i < RUNS; i++) {
            Instant scheduled = T0.plus(Duration.ofSeconds(5L * i));
            events.add(new JobRunEvent("p1", "run-" + i, EventType.STARTED, null,
                    scheduled, scheduled.plusSeconds(30)));
            events.add(new JobRunEvent("p1", "run-" + i, EventType.FINISHED,
                    i % 10 == 0 ? RunStatus.FAILED : RunStatus.SUCCESS,
                    scheduled, scheduled.plusSeconds(90)));
        }
        return events;
    }

    @Test
    void concurrentIngestAndEvaluateIsConsistent() throws InterruptedException {
        SlaProperties props = props();
        EventStore store = new EventStore(props);
        SlaEvaluator evaluator = new SlaEvaluator(store, props);
        AlertManager alerts = new AlertManager(props);

        List<JobRunEvent> events = fixture();
        int distinct = events.size();

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        Map<IngestStatus, AtomicInteger> counts = Map.of(
                IngestStatus.ACCEPTED, accepted, IngestStatus.DUPLICATE, duplicate, IngestStatus.REJECTED, rejected);

        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch start = new CountDownLatch(1);

        // 8 个上报线程：每条事件上报两次（乱序穿插），验证并发幂等
        for (int t = 0; t < 8; t++) {
            final int offset = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 2; round++) {
                        for (int i = offset; i < events.size(); i += 8) {
                            var r = store.ingest(events.get(i));
                            counts.get(r.status()).incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        // 2 个评估线程：并发评估并推进告警
        for (int t = 0; t < 2; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 50; i++) {
                        alerts.onAssessment(evaluator.evaluate("p1"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发任务应在限时内完成");

        System.out.println("[ConcurrencyTest] 上报统计: accepted=" + accepted.get()
                + " duplicate=" + duplicate.get() + " rejected=" + rejected.get()
                + " distinct=" + distinct);

        // 幂等：每条不同事件恰好接受一次、重复一次，零拒绝
        assertEquals(distinct, accepted.get(), "每条不同事件必须恰好被接受一次");
        assertEquals(distinct, duplicate.get(), "重复上报必须全部识别为 DUPLICATE");
        assertEquals(0, rejected.get(), "合法事件不得被拒绝");

        // 告警：alertId 唯一，每个指标最多一个 FIRING，状态不倒退
        List<Alert> all = alerts.allAlerts();
        System.out.println("[ConcurrencyTest] 告警快照=" + all);
        var ids = all.stream().map(Alert::alertId).collect(Collectors.toSet());
        assertEquals(all.size(), ids.size(), "alertId 不得重复");
        Map<Object, Long> firingByMetric = alerts.activeAlerts().stream()
                .collect(Collectors.groupingBy(Alert::metricType, Collectors.counting()));
        firingByMetric.values().forEach(c -> assertEquals(1L, c, "同一指标不得并发出多个 FIRING"));
        all.stream().filter(a -> a.status() == AlertStatus.RESOLVED)
                .forEach(a -> assertFalse(a.resolvedAt().isBefore(a.firedAt()), "恢复时间不得早于触发时间"));

        // 确定性：同样的事件集顺序重放，评估结论必须与并发场景一致
        EventStore replayStore = new EventStore(props());
        SlaEvaluator replayEvaluator = new SlaEvaluator(replayStore, props());
        events.forEach(replayStore::ingest);
        PipelineAssessment concurrent = evaluator.evaluate("p1");
        PipelineAssessment replayed = replayEvaluator.evaluate("p1");
        System.out.println("[ConcurrencyTest] 并发评估=" + concurrent);
        System.out.println("[ConcurrencyTest] 重放评估=" + replayed);
        assertEquals(replayed, concurrent, "并发上报后的评估结论必须与顺序重放一致");
    }
}
