package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** 并发上报 + 并发评估 + 并发维护声明 + 并发认领：不丢告警、不重复触发、状态不倒退。 */
class OperationsConcurrencyTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");
    private static final int RUNS = 200;

    @Test
    void concurrentIngestEvaluateMaintenanceAndAck() throws InterruptedException {
        SlaProperties props = new SlaProperties();
        props.getDefaultRule().setMinSlaAttainment(0.9);
        props.getDefaultRule().setEscalateAfter(3);
        EventStore store = new EventStore(props);
        SlaEvaluator evaluator = new SlaEvaluator(store, props);
        MaintenanceService maintenance = new MaintenanceService();
        AlertManager alerts = new AlertManager(props, maintenance);

        List<JobRunEvent> events = new java.util.ArrayList<>();
        for (int i = 0; i < RUNS; i++) {
            Instant at = T0.plus(Duration.ofSeconds(10L * i));
            events.add(new JobRunEvent("pc", "run-" + i, EventType.FINISHED,
                    i % 2 == 0 ? RunStatus.FAILED : RunStatus.SUCCESS, at, at.plusSeconds(5)));
        }

        AtomicInteger acked = new AtomicInteger();
        AtomicInteger ackDup = new AtomicInteger();
        AtomicInteger ackRejected = new AtomicInteger();
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);

        // 4 个上报线程
        for (int t = 0; t < 4; t++) {
            final int offset = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = offset; i < events.size(); i += 4) {
                        store.ingest(events.get(i));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.add("ingest: " + e);
                }
            });
        }
        // 2 个评估线程
        for (int t = 0; t < 2; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        alerts.onAssessment(evaluator.evaluate("pc"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.add("evaluate: " + e);
                }
            });
        }
        // 3 个维护声明/撤销线程（同一窗口反复声明与撤销）
        Instant mStart = T0.plus(Duration.ofSeconds(500));
        Instant mEnd = T0.plus(Duration.ofSeconds(1000));
        for (int t = 0; t < 3; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 50; i++) {
                        maintenance.declare("pc", null, mStart, mEnd, "并发窗口");
                        if (i % 3 == 0) {
                            maintenance.revoke("pc", null, mStart, mEnd);
                            maintenance.declare("pc", null, mStart, mEnd, "并发窗口");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.add("maintenance: " + e);
                }
            });
        }
        // 3 个认领线程：反复认领当前活动告警
        for (int t = 0; t < 3; t++) {
            final String who = "oncall-" + t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        for (Alert a : alerts.activeAlerts()) {
                            AckResult r = alerts.ack(a.alertId(), who, "并发认领", T0);
                            switch (r.status()) {
                                case ACKNOWLEDGED -> acked.incrementAndGet();
                                case DUPLICATE -> ackDup.incrementAndGet();
                                case REJECTED -> ackRejected.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.add("ack: " + e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发任务应在限时内完成");
        assertTrue(errors.isEmpty(), "并发过程不得抛异常: " + errors);

        List<Alert> all = alerts.allAlerts();
        System.out.println("[OpsConcurrency] 告警数=" + all.size()
                + " acked=" + acked.get() + " dup=" + ackDup.get() + " rejected=" + ackRejected.get()
                + " 维护窗口数=" + maintenance.list("pc").size());

        // alertId 唯一
        var ids = all.stream().map(Alert::alertId).collect(Collectors.toSet());
        assertEquals(all.size(), ids.size(), "alertId 不得重复");
        // 每指标至多一个 FIRING
        Map<Object, Long> firingByMetric = alerts.activeAlerts().stream()
                .collect(Collectors.groupingBy(Alert::metricType, Collectors.counting()));
        firingByMetric.values().forEach(c -> assertEquals(1L, c, "同一指标不得并发出多个 FIRING"));
        // 状态不倒退：恢复时间不早于触发时间
        all.stream().filter(a -> a.status() == AlertStatus.RESOLVED)
                .forEach(a -> assertFalse(a.resolvedAt().isBefore(a.firedAt())));
        // 认领一致性：每条告警的认领人要么为空要么唯一（并发认领只会有一个赢家）
        long ackedAlerts = all.stream().filter(a -> a.acknowledgedBy() != null).count();
        System.out.println("[OpsConcurrency] 被认领告警数=" + ackedAlerts);
        assertEquals(acked.get(), ackedAlerts, "ACKNOWLEDGED 次数必须等于被认领的告警数（不丢不重）");
        // 维护窗口：同一窗口并发声明最终至多一条记录
        assertTrue(maintenance.list("pc").size() <= 1, "同一维护窗口并发声明不得产生多份记录");
    }
}
