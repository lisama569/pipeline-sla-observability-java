package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MaintenanceServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");
    private static final Instant START = T0.plus(Duration.ofHours(1));
    private static final Instant END = T0.plus(Duration.ofHours(2));

    private MaintenanceService service;

    @BeforeEach
    void setUp() {
        service = new MaintenanceService();
    }

    private void log(String phase, MaintenanceResult r) {
        System.out.println("[MaintenanceTest] " + phase + " -> " + r);
    }

    @Test
    void declareIsIdempotentAndQueryable() {
        MaintenanceResult r1 = service.declare("orders", null, START, END, "换版窗口");
        log("首次声明", r1);
        assertEquals(MaintenanceStatus.DECLARED, r1.status());

        MaintenanceResult r2 = service.declare("orders", null, START, END, "换版窗口");
        log("重复声明", r2);
        assertEquals(MaintenanceStatus.DUPLICATE, r2.status(), "相同窗口重复声明必须幂等");
        assertEquals(r1.window().windowId(), r2.window().windowId());
        assertEquals(1, service.list("orders").size(), "重复声明不得产生第二份记录");

        // 静默判断按事件时间：[start, end)
        assertFalse(service.isSuppressed("orders", MetricType.FRESHNESS, START.minusSeconds(1)),
                "窗口开始前不静默");
        assertTrue(service.isSuppressed("orders", MetricType.FRESHNESS, START), "窗口起点（含）静默");
        assertTrue(service.isSuppressed("orders", MetricType.SLA_ATTAINMENT, END.minusSeconds(1)));
        assertFalse(service.isSuppressed("orders", MetricType.FRESHNESS, END), "窗口终点（不含）不再静默");
        assertFalse(service.isSuppressed("other", MetricType.FRESHNESS, START),
                "其它流水线不受该窗口影响");
    }

    @Test
    void metricScopedWindowOnlySuppressesThatMetric() {
        MaintenanceResult r = service.declare("orders", MetricType.SLA_ATTAINMENT, START, END, null);
        log("按指标声明", r);
        assertEquals(MaintenanceStatus.DECLARED, r.status());

        assertTrue(service.isSuppressed("orders", MetricType.SLA_ATTAINMENT, START.plusSeconds(60)));
        assertFalse(service.isSuppressed("orders", MetricType.FRESHNESS, START.plusSeconds(60)),
                "未指定的指标不得被静默");
        assertFalse(service.isSuppressed("orders", MetricType.SCHEDULE_DELAY, START.plusSeconds(60)));
    }

    @Test
    void invalidDeclarationsAreRejectedWithDistinctReasons() {
        MaintenanceResult missing = service.declare(" ", null, START, END, null);
        log("缺 pipelineId", missing);
        assertEquals(MaintenanceStatus.REJECTED, missing.status());
        assertEquals(MaintenanceRejectReason.MISSING_FIELDS, missing.reason());

        MaintenanceResult nullTimes = service.declare("orders", null, null, END, null);
        log("缺时间", nullTimes);
        assertEquals(MaintenanceRejectReason.MISSING_FIELDS, nullTimes.reason());

        MaintenanceResult inverted = service.declare("orders", null, END, START, null);
        log("end<start", inverted);
        assertEquals(MaintenanceStatus.REJECTED, inverted.status());
        assertEquals(MaintenanceRejectReason.INVALID_TIME_RANGE, inverted.reason());

        MaintenanceResult empty = service.declare("orders", null, START, START, null);
        log("end==start", empty);
        assertEquals(MaintenanceRejectReason.INVALID_TIME_RANGE, empty.reason());

        assertTrue(service.list(null).isEmpty(), "被拒绝的声明不得留下任何记录");
    }

    @Test
    void revokeRemovesWindowAndIsDistinguishable() {
        service.declare("orders", null, START, END, null);
        MaintenanceResult revoked = service.revoke("orders", null, START, END);
        log("撤销", revoked);
        assertEquals(MaintenanceStatus.REVOKED, revoked.status());
        assertFalse(service.isSuppressed("orders", MetricType.FRESHNESS, START.plusSeconds(60)),
                "撤销后窗口不再静默");

        MaintenanceResult again = service.revoke("orders", null, START, END);
        log("重复撤销", again);
        assertEquals(MaintenanceStatus.NOT_FOUND, again.status(), "撤销不存在的窗口应可区分");

        // 撤销后重新声明 = 新记录
        MaintenanceResult redeclared = service.declare("orders", null, START, END, null);
        log("撤销后重声明", redeclared);
        assertEquals(MaintenanceStatus.DECLARED, redeclared.status());
    }

    @Test
    void concurrentDeclareAndRevokeAreConsistent() throws InterruptedException {
        int declarers = 4;
        int revokers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(declarers + revokers);
        CountDownLatch start = new CountDownLatch(1);
        List<MaintenanceResult> declareResults = new CopyOnWriteArrayList<>();
        // 一半线程只声明（同一窗口），另一半只撤销，并发交错
        for (int t = 0; t < declarers; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    declareResults.add(service.declare("orders", null, START, END, "并发"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        for (int t = 0; t < revokers; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    service.revoke("orders", null, START, END);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long declared = declareResults.stream().filter(r -> r.status() == MaintenanceStatus.DECLARED).count();
        long duplicate = declareResults.stream().filter(r -> r.status() == MaintenanceStatus.DUPLICATE).count();
        System.out.println("[MaintenanceTest] 并发声明: declared=" + declared + " duplicate=" + duplicate
                + " 最终窗口数=" + service.list("orders").size());
        assertEquals(declarers, declared + duplicate, "声明结果只能是 DECLARED 或 DUPLICATE，不得丢失");
        assertTrue(declared >= 1, "首次声明必须生效");
        assertTrue(service.list("orders").size() <= 1,
                "无论声明/撤销如何交错，同一窗口至多一条记录，不得出现重复");
    }
}
