package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.AlertState;
import com.github.lisama569.pipelineslaobservabilityjava.domain.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestOutcome;
import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.domain.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.domain.Thresholds;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.EventReport;
import com.github.lisama569.pipelineslaobservabilityjava.store.PipelineState;
import com.github.lisama569.pipelineslaobservabilityjava.tests.support.ServiceTestBase;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景：上报线程与评估线程同时运行，验证
 * 事件不丢不重、接收序号连续、每个 run 终态唯一、告警不丢失/不重复/状态不倒退。
 */
class ConcurrencyTest extends ServiceTestBase {

    private static final int REPORTER_THREADS = 8;
    private static final int RUNS_PER_THREAD = 100;

    @BeforeEach
    void setUp() {
        Thresholds th = new Thresholds(60, 10_000, 100, 30, 30);
        register("p1", 3600, 7200, 3600, th);
        register("p2", 3600, 7200, 3600, th);
        clock.set(T0);
    }

    @Test
    @DisplayName("并发上报+并发评估：事件不丢不重、序号连续、无重复终态、告警迁移合法")
    void concurrentIngestAndEvaluateIsSafe() throws Exception {
        int threads = REPORTER_THREADS;
        int perThread = RUNS_PER_THREAD;
        int totalRuns = threads * perThread;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger acceptedTerminals = new AtomicInteger();
        Set<String> duplicateTerminalRejections = ConcurrentHashMap.newKeySet();

        try {
            // 上报线程：每个 run 一个 STARTED + 一个 SUCCEEDED/FAILED；约 1/4 失败以制造告警
            for (int t = 0; t < threads; t++) {
                final int threadIdx = t;
                pool.submit(() -> {
                    awaitQuietly(start);
                    for (int i = 0; i < perThread; i++) {
                        String pipeline = (threadIdx + i) % 2 == 0 ? "p1" : "p2";
                        String runId = "run-" + threadIdx + "-" + i;
                        Instant runTime = T0.plusSeconds((long) (threadIdx * perThread + i));
                        EventReport started = event(pipeline, runId, runId + "-start",
                                "STARTED", runTime, runTime);
                        IngestResult rs = ingestService.report(started);
                        assertTrue(rs.outcome() != IngestOutcome.REJECTED,
                                "STARTED 不应被拒绝: " + rs.message());

                        boolean failRun = i % 4 == 0;
                        EventReport terminal = event(pipeline, runId, runId + "-end",
                                failRun ? "FAILED" : "SUCCEEDED", runTime.plusSeconds(10), null);
                        IngestResult rt = ingestService.report(terminal);
                        if (rt.outcome() == IngestOutcome.ACCEPTED) {
                            acceptedTerminals.incrementAndGet();
                        } else if (rt.reason() == RejectReason.CONFLICT) {
                            duplicateTerminalRejections.add(runId);
                        }
                    }
                });
            }

            // 评估线程：与上报并发持续评估
            AtomicInteger evaluations = new AtomicInteger();
            for (int e = 0; e < 2; e++) {
                pool.submit(() -> {
                    awaitQuietly(start);
                    while (evaluations.incrementAndGet() <= 400) {
                        evaluationService.evaluateAll();
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                });
            }

            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发任务必须在超时前结束");
        } finally {
            pool.shutdownNow();
        }

        testLog.info("concurrency finished: terminal accepted={}, spurious-conflicts={}",
                acceptedTerminals.get(), duplicateTerminalRejections.size());

        // ---- 事件不丢不重：两个流水线合计 totalRuns 个 run、2×totalRuns 个事件 ----
        for (String pipeline : List.of("p1", "p2")) {
            PipelineState state = registry.find(pipeline).orElseThrow();
            synchronized (state) {
                int expectedRuns = 0;
                int expectedEvents = 0;
                for (JobRunEvent ev : state.getEventsById().values()) {
                    expectedEvents++;
                    if (ev.eventType() == EventType.STARTED) {
                        expectedRuns++;
                    }
                }
                assertEquals(expectedRuns, state.getRuns().size(),
                        pipeline + ": run 聚合数必须与 STARTED 事件数一致");
                // 每个 run 终态唯一
                long terminalCount = state.getEventsById().values().stream()
                        .filter(ev -> ev.eventType() != EventType.STARTED).count();
                long runsWithTerminal = state.getRuns().values().stream()
                        .filter(r -> r.getTerminalType() != null).count();
                assertEquals(terminalCount, runsWithTerminal,
                        pipeline + ": 每个 run 至多一个终态，不得重复计数");
                testLog.info("{} events={}, runs={}, terminals={}", pipeline, expectedEvents,
                        expectedRuns, terminalCount);
            }
        }

        // 全局接收序号连续：取所有事件序号，必须是从 1 开始无空洞的集合
        List<Long> allSeqs = new ArrayList<>();
        for (String pipeline : List.of("p1", "p2")) {
            PipelineState state = registry.find(pipeline).orElseThrow();
            synchronized (state) {
                state.getEventsById().values().forEach(ev -> allSeqs.add(ev.ingestSeq()));
            }
        }
        allSeqs.sort(Long::compareTo);
        assertEquals(totalRuns * 2L, allSeqs.size(), "总事件数必须精确（不丢不重）");
        for (int i = 0; i < allSeqs.size(); i++) {
            assertEquals(i + 1L, allSeqs.get(i), "接收序号必须连续无空洞，位置 " + i);
        }

        // 不应有“伪 CONFLICT”：每个 run 只上报一次终态
        assertTrue(duplicateTerminalRejections.isEmpty(),
                "唯一终态上报不应触发 CONFLICT: " + duplicateTerminalRejections);
        assertEquals(totalRuns, acceptedTerminals.get());

        // 最终强制评估一次，检查告警状态机合法性：迁移只允许前进，不允许倒退
        for (String pipeline : List.of("p1", "p2")) {
            evaluationService.evaluate(pipeline);
            PipelineState state = registry.find(pipeline).orElseThrow();
            synchronized (state) {
                for (Alert alert : state.getActiveAlerts().values()) {
                    assertTransitionsMonotonic(alert);
                }
                for (Alert alert : state.getRecoveredAlerts()) {
                    assertTransitionsMonotonic(alert);
                    assertEquals(AlertState.RECOVERED, alert.getState());
                }
            }
        }
        testLog.info("concurrency alert invariants verified");
    }

    @Test
    @DisplayName("同一事件并发重复上报：恰好一次 ACCEPTED，其余全部 DUPLICATE")
    void concurrentDuplicateSameEventId() throws Exception {
        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<IngestResult> results = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                awaitQuietly(start);
                IngestResult r = ingestService.report(event(
                        "p1", "run-dup", "dup-event", "STARTED", T0, T0));
                synchronized (results) {
                    results.add(r);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long accepted = results.stream().filter(r -> r.outcome() == IngestOutcome.ACCEPTED).count();
        long duplicate = results.stream().filter(r -> r.outcome() == IngestOutcome.DUPLICATE).count();
        testLog.info("concurrent duplicates: accepted={}, duplicate={}", accepted, duplicate);
        assertEquals(1, accepted, "同一事件并发上报必须恰好接收一次");
        assertEquals(n - 1, duplicate, "其余必须判定为幂等重复");
        long distinctSeq = results.stream().map(IngestResult::ingestSeq).distinct().count();
        assertEquals(1, distinctSeq, "所有结论必须指向同一个接收序号");
    }

    private static void assertTransitionsMonotonic(Alert alert) {
        AlertState prev = null;
        for (var t : alert.getTransitions()) {
            if (prev != null) {
                assertTrue(ordinal(t.from()) == ordinal(prev),
                        "迁移 from 必须等于当前状态: " + alert.getTransitions());
                assertTrue(ordinal(t.to()) > ordinal(prev),
                        "告警状态只允许前进，不得倒退: " + alert.getTransitions());
            }
            prev = t.to();
        }
    }

    private static int ordinal(AlertState s) {
        return s.ordinal();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
