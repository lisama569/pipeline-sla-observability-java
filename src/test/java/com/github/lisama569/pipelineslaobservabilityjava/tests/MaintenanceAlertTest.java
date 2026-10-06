package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.Severity;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import com.github.lisama569.pipelineslaobservabilityjava.service.MaintenanceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 维护静默与告警生命周期的交互：静默期内不新拉、不升级，静默结束后照常触发。 */
class MaintenanceAlertTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");

    private MaintenanceService maintenance;
    private AlertManager manager;

    @BeforeEach
    void setUp() {
        SlaProperties props = new SlaProperties();
        SlaProperties.PipelineRule rule = props.getDefaultRule();
        rule.setMinSlaAttainment(0.9);
        rule.setRecoveryMargin(0.05);
        rule.setEscalateAfter(3);
        maintenance = new MaintenanceService();
        manager = new AlertManager(props, maintenance);
    }

    private PipelineAssessment slaAssessment(double attainment, boolean breached, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment("p1", at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, attainment, breached, List.of());
        System.out.println("[MaintenanceAlertTest] 评估: attainment=" + attainment
                + " breached=" + breached + " at=" + at);
        return a;
    }

    private void logAlerts(String phase) {
        System.out.println("[MaintenanceAlertTest] " + phase + " 告警=" + manager.allAlerts());
    }

    @Test
    void breachDuringMaintenanceDoesNotFireButFiresAfterWindowEnds() {
        // 维护窗口 [10min, 30min)
        maintenance.declare("p1", null, T0.plus(Duration.ofMinutes(10)),
                T0.plus(Duration.ofMinutes(30)), "计划换版");

        manager.onAssessment(slaAssessment(0.8, true, 5));
        logAlerts("窗口前违约");
        assertEquals(1, manager.activeAlerts().size(), "窗口外的违约应正常触发");
        // 先恢复，为窗口内违约做准备
        manager.onAssessment(slaAssessment(0.99, false, 8));
        assertEquals(0, manager.activeAlerts().size());

        // 窗口内持续违约：不新拉告警
        manager.onAssessment(slaAssessment(0.7, true, 15));
        manager.onAssessment(slaAssessment(0.7, true, 20));
        manager.onAssessment(slaAssessment(0.7, true, 25));
        logAlerts("窗口内违约");
        assertEquals(0, manager.activeAlerts().size(), "维护窗口内不得新拉告警");
        assertEquals(1, manager.allAlerts().size(), "窗口内违约不得产生新告警记录");

        // 窗口结束后仍违约：必须形成新告警
        manager.onAssessment(slaAssessment(0.7, true, 31));
        logAlerts("窗口后违约");
        assertEquals(1, manager.activeAlerts().size(), "维护结束后仍违约必须触发新告警");
        assertEquals(2, manager.allAlerts().size());
        assertEquals(Severity.WARNING, manager.activeAlerts().get(0).severity());
    }

    @Test
    void maintenanceSuppressesEscalationButNotRecovery() {
        maintenance.declare("p1", null, T0.plus(Duration.ofMinutes(10)),
                T0.plus(Duration.ofMinutes(30)), null);

        manager.onAssessment(slaAssessment(0.8, true, 0));
        manager.onAssessment(slaAssessment(0.8, true, 5));
        // 窗口内继续违约：不升级（escalateAfter=3，第 3 次违约落在窗口内）
        manager.onAssessment(slaAssessment(0.8, true, 15));
        manager.onAssessment(slaAssessment(0.8, true, 20));
        logAlerts("窗口内持续违约");
        Alert inWindow = manager.activeAlerts().get(0);
        assertEquals(Severity.WARNING, inWindow.severity(), "维护窗口内不得升级");
        assertEquals(0, inWindow.escalationCount());

        // 窗口结束后继续违约：升级恢复计数（窗口内违约不计入连续次数）
        manager.onAssessment(slaAssessment(0.8, true, 31));
        manager.onAssessment(slaAssessment(0.8, true, 32));
        manager.onAssessment(slaAssessment(0.8, true, 33));
        logAlerts("窗口后持续违约");
        Alert after = manager.activeAlerts().get(0);
        assertEquals(Severity.CRITICAL, after.severity(), "窗口结束后连续违约达到阈值应升级");
        assertEquals(1, manager.allAlerts().size(), "全程不得产生第二条告警");

        // 恢复不受静默影响：再声明一个窗口，带裕量恢复仍生效
        maintenance.declare("p1", null, T0.plus(Duration.ofMinutes(40)),
                T0.plus(Duration.ofMinutes(50)), null);
        manager.onAssessment(slaAssessment(0.99, false, 45));
        logAlerts("窗口内恢复");
        assertEquals(0, manager.activeAlerts().size(), "维护窗口内的带裕量恢复应照常生效");
        assertEquals(AlertStatus.RESOLVED, manager.allAlerts().get(0).status());
    }

    @Test
    void metricScopedWindowDoesNotAffectOtherMetrics() {
        // 只静默 SLA_ATTAINMENT
        maintenance.declare("p1", MetricType.SLA_ATTAINMENT, T0, T0.plus(Duration.ofHours(1)), null);

        // SLA 违约被静默
        manager.onAssessment(slaAssessment(0.7, true, 10));
        assertTrue(manager.activeAlerts().isEmpty(), "被静默的指标不得触发");

        // FRESHNESS 违约照常触发
        Instant at = T0.plus(Duration.ofMinutes(20));
        var freshnessBreach = new PipelineAssessment("p1", at, at.minus(Duration.ofHours(1)),
                true, 9999L, 0L, false, null, false, List.of());
        System.out.println("[MaintenanceAlertTest] 评估: freshnessBreached=true at=" + at);
        manager.onAssessment(freshnessBreach);
        logAlerts("指标级静默下其它指标违约");
        assertEquals(1, manager.activeAlerts().size(), "未静默的指标应照常触发");
        assertEquals(MetricType.FRESHNESS, manager.activeAlerts().get(0).metricType());
    }

    @Test
    void revokedWindowNoLongerSuppresses() {
        Instant start = T0.plus(Duration.ofMinutes(10));
        Instant end = T0.plus(Duration.ofMinutes(30));
        maintenance.declare("p1", null, start, end, null);
        maintenance.revoke("p1", null, start, end);

        manager.onAssessment(slaAssessment(0.7, true, 15));
        logAlerts("撤销后窗口内违约");
        assertEquals(1, manager.activeAlerts().size(), "窗口被撤销后不再静默");
    }
}
