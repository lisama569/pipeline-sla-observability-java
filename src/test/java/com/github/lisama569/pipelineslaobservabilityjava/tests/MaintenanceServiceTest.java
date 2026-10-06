package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Severity;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService;
import com.github.lisama569.pipelineslaobservabilityjava.service.SlaEvaluator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MaintenanceServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");

    private MaintenanceService maintenance;
    private AlertManager manager;

    @BeforeEach
    void setUp() {
        maintenance = new MaintenanceService();
        SlaProperties props = new SlaProperties();
        props.getDefaultRule().setEscalateAfter(3);
        manager = new AlertManager(props, maintenance);
    }

    private MaintenanceWindow window(String id, String pipeline, MetricType metric, int startMin, int endMin) {
        var w = new MaintenanceWindow(id, pipeline, metric,
                T0.plus(Duration.ofMinutes(startMin)), T0.plus(Duration.ofMinutes(endMin)), "计划维护");
        System.out.println("[MaintenanceTest] 声明窗口: " + w);
        return w;
    }

    private PipelineAssessment slaBreach(String pipeline, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment(pipeline, at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, 0.5, true, List.of());
        System.out.println("[MaintenanceTest] 评估: pipeline=" + pipeline + " SLA 违约 at=" + at);
        return a;
    }

    private PipelineAssessment slaRecovered(String pipeline, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment(pipeline, at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, 0.99, false, List.of());
        System.out.println("[MaintenanceTest] 评估: pipeline=" + pipeline + " 带裕量恢复 at=" + at);
        return a;
    }

    @Test
    void declareIsIdempotentAndConflictIsDistinguishable() {
        var w = window("mw-1", "p1", null, 60, 120);
        var first = maintenance.declare(w);
        System.out.println("[MaintenanceTest] 首次声明 -> " + first);
        assertEquals(MaintenanceStatus.DECLARED, first.status());

        var dup = maintenance.declare(w);
        System.out.println("[MaintenanceTest] 重复声明 -> " + dup);
        assertEquals(MaintenanceStatus.DUPLICATE, dup.status(), "同 id 同内容必须幂等");
        assertEquals(1, maintenance.list().size(), "重复声明不得产生第二份记录");

        var conflict = maintenance.declare(new MaintenanceWindow("mw-1", "p1", null,
                T0.plus(Duration.ofMinutes(30)), T0.plus(Duration.ofMinutes(90)), "改动内容"));
        System.out.println("[MaintenanceTest] 同 id 不同内容 -> " + conflict);
        assertEquals(MaintenanceStatus.REJECTED, conflict.status());
        assertEquals(MaintenanceRejectReason.CONFLICTING_WINDOW, conflict.reason());
        assertEquals(1, maintenance.list().size(), "冲突声明不得覆盖既有窗口");

        var invalid = maintenance.declare(new MaintenanceWindow("mw-2", "p1", null,
                T0.plus(Duration.ofMinutes(120)), T0.plus(Duration.ofMinutes(60)), "start 晚于 end"));
        System.out.println("[MaintenanceTest] 非法时间段 -> " + invalid);
        assertEquals(MaintenanceStatus.REJECTED, invalid.status());
        assertEquals(MaintenanceRejectReason.INVALID_WINDOW, invalid.reason(),
                "非法时间段与冲突的原因必须可区分");

        var revoked = maintenance.revoke("mw-1");
        System.out.println("[MaintenanceTest] 撤销 -> " + revoked);
        assertEquals(MaintenanceStatus.REVOKED, revoked.status());
        assertEquals(MaintenanceStatus.NOT_FOUND, maintenance.revoke("mw-1").status(),
                "重复撤销必须返回可区分的 NOT_FOUND");
        assertTrue(maintenance.list().isEmpty());
    }

    @Test
    void suppressionScopesByPipelineMetricAndEventTime() {
        maintenance.declare(window("mw-all", "p1", null, 60, 120));
        maintenance.declare(window("mw-sla", "p2", MetricType.SLA_ATTAINMENT, 60, 120));

        Instant inside = T0.plus(Duration.ofMinutes(90));
        Instant before = T0.plus(Duration.ofMinutes(59));
        Instant atEnd = T0.plus(Duration.ofMinutes(120));
        System.out.println("[MaintenanceTest] 判定时间点: inside=" + inside + " before=" + before + " atEnd=" + atEnd);

        assertTrue(maintenance.isSuppressed("p1", MetricType.FRESHNESS, inside), "全指标窗口应覆盖 FRESHNESS");
        assertTrue(maintenance.isSuppressed("p1", MetricType.SLA_ATTAINMENT, inside));
        assertFalse(maintenance.isSuppressed("p1", MetricType.FRESHNESS, before), "窗口开始前不得静默");
        assertFalse(maintenance.isSuppressed("p1", MetricType.FRESHNESS, atEnd), "end 为开区间，到点即解除");

        assertTrue(maintenance.isSuppressed("p2", MetricType.SLA_ATTAINMENT, inside), "指标级窗口应覆盖指定指标");
        assertFalse(maintenance.isSuppressed("p2", MetricType.FRESHNESS, inside), "指标级窗口不得波及其他指标");
        assertFalse(maintenance.isSuppressed("p3", MetricType.SLA_ATTAINMENT, inside),
                "其他流水线不得被静默（流水线之间隔离）");
    }

    @Test
    void breachDuringWindowIsSuppressedAndFiresAfterWindowEnds() {
        maintenance.declare(window("mw-1", "p1", null, 1, 3));

        manager.onAssessment(slaBreach("p1", 1));
        manager.onAssessment(slaBreach("p1", 2));
        System.out.println("[MaintenanceTest] 窗口内违约后告警=" + manager.allAlerts());
        assertTrue(manager.allAlerts().isEmpty(), "维护窗口内违约不得新拉告警");

        manager.onAssessment(slaBreach("p1", 3)); // 窗口结束（end 开区间），仍违约
        System.out.println("[MaintenanceTest] 窗口结束后告警=" + manager.allAlerts());
        assertEquals(1, manager.activeAlerts().size(), "窗口过后仍违约必须形成新告警");
        assertEquals(T0.plus(Duration.ofMinutes(3)), manager.activeAlerts().get(0).firedAt());
    }

    @Test
    void existingAlertIsNotEscalatedDuringWindow() {
        manager.onAssessment(slaBreach("p1", 0)); // 窗口外触发，count=1
        assertEquals(1, manager.activeAlerts().size());

        maintenance.declare(window("mw-1", "p1", null, 1, 3));
        manager.onAssessment(slaBreach("p1", 1)); // 静默：不计数不升级
        manager.onAssessment(slaBreach("p1", 2)); // 静默：不计数不升级
        Alert during = manager.activeAlerts().get(0);
        System.out.println("[MaintenanceTest] 窗口内既有告警=" + during);
        assertEquals(Severity.WARNING, during.severity(), "维护窗口内不得升级既有告警");
        assertEquals(1, manager.allAlerts().size(), "维护窗口内不得产生额外告警");

        manager.onAssessment(slaBreach("p1", 3)); // count=2
        assertEquals(Severity.WARNING, manager.activeAlerts().get(0).severity(),
                "静默期不计入连续违约次数");
        manager.onAssessment(slaBreach("p1", 4)); // count=3 → 升级
        Alert after = manager.activeAlerts().get(0);
        System.out.println("[MaintenanceTest] 窗口结束后告警=" + after);
        assertEquals(Severity.CRITICAL, after.severity(), "窗口结束后正常计数升级");
    }

    @Test
    void recoveryDuringWindowStillApplies() {
        manager.onAssessment(slaBreach("p1", 0));
        maintenance.declare(window("mw-1", "p1", null, 1, 3));
        manager.onAssessment(slaRecovered("p1", 2)); // 窗口内恢复健康
        System.out.println("[MaintenanceTest] 窗口内恢复后告警=" + manager.allAlerts());
        assertEquals(AlertStatus.RESOLVED, manager.allAlerts().get(0).status(),
                "维护只按住违约侧，正常恢复不受影响");
    }

    @Test
    void assessmentRemainsQueryableDuringWindow() {
        // 端到端：事件照打、评估结论照常违约，只是不产生告警
        SlaProperties props = new SlaProperties();
        props.getDefaultRule().setMinSlaAttainment(0.9);
        EventStore store = new EventStore(props);
        SlaEvaluator evaluator = new SlaEvaluator(store, props);
        AlertManager alerts = new AlertManager(props, maintenance);

        maintenance.declare(window("mw-1", "orders", null, 0, 120));
        var failed = new JobRunEvent("orders", "r1", EventType.FINISHED, RunStatus.FAILED,
                T0, T0.plus(Duration.ofMinutes(30)));
        System.out.println("[MaintenanceTest] 上报事件: " + failed);
        store.ingest(failed);

        PipelineAssessment a = evaluator.evaluate("orders");
        System.out.println("[MaintenanceTest] 窗口内评估结论=" + a);
        assertTrue(a.slaBreached(), "维护窗口内评估结论必须照常可查（违约仍是违约）");
        alerts.onAssessment(a);
        assertTrue(alerts.allAlerts().isEmpty(), "但不得新拉告警");
    }
}
