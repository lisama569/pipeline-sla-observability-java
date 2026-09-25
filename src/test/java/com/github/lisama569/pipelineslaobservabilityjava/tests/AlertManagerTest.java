package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.Severity;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AlertManagerTest {

    private static final Instant T0 = Instant.parse("2026-09-26T00:00:00Z");

    private AlertManager manager;

    @BeforeEach
    void setUp() {
        SlaProperties props = new SlaProperties();
        SlaProperties.PipelineRule rule = props.getDefaultRule();
        rule.setMinSlaAttainment(0.9);
        rule.setRecoveryMargin(0.05); // 恢复需 >= 0.95
        rule.setEscalateAfter(3);
        manager = new AlertManager(props);
    }

    /** 构造只含 SLA 达标率信号的评估结论。 */
    private PipelineAssessment slaAssessment(double attainment, boolean breached, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment("p1", at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, attainment, breached, List.of());
        System.out.println("[AlertManagerTest] 评估: attainment=" + attainment
                + " breached=" + breached + " at=" + at);
        return a;
    }

    private void logAlerts(String phase) {
        System.out.println("[AlertManagerTest] " + phase + " 当前告警=" + manager.allAlerts());
    }

    @Test
    void flappingAroundThresholdDoesNotDuplicateAlerts() {
        // 违约触发
        manager.onAssessment(slaAssessment(0.8, true, 0));
        assertEquals(1, manager.activeAlerts().size());

        // 阈值附近抖动：0.90~0.95 之间（不违约但未带裕量恢复）→ 保持 FIRING，不新增不恢复
        manager.onAssessment(slaAssessment(0.92, false, 1));
        manager.onAssessment(slaAssessment(0.89, true, 2));
        manager.onAssessment(slaAssessment(0.93, false, 3));
        logAlerts("抖动后");
        assertEquals(1, manager.allAlerts().size(), "抖动不得产生重复告警");
        assertEquals(AlertStatus.FIRING, manager.allAlerts().get(0).status());

        // 带裕量恢复（>=0.95）→ 自动恢复
        manager.onAssessment(slaAssessment(0.97, false, 4));
        logAlerts("恢复后");
        assertEquals(0, manager.activeAlerts().size());
        assertEquals(AlertStatus.RESOLVED, manager.allAlerts().get(0).status());
        assertEquals(T0.plus(Duration.ofMinutes(4)), manager.allAlerts().get(0).resolvedAt());
    }

    @Test
    void sustainedBreachEscalatesToCritical() {
        manager.onAssessment(slaAssessment(0.8, true, 0));
        manager.onAssessment(slaAssessment(0.8, true, 1));
        assertEquals(Severity.WARNING, manager.activeAlerts().get(0).severity());
        manager.onAssessment(slaAssessment(0.8, true, 2)); // 第 3 次连续违约 → 升级
        logAlerts("升级后");
        Alert alert = manager.activeAlerts().get(0);
        assertEquals(Severity.CRITICAL, alert.severity());
        assertEquals(1, alert.escalationCount());
        assertEquals(1, manager.allAlerts().size(), "升级不得产生新告警");
    }

    @Test
    void recoveryThenNewBreachStartsNewLifecycle() {
        manager.onAssessment(slaAssessment(0.8, true, 0));
        manager.onAssessment(slaAssessment(0.99, false, 1)); // 恢复
        manager.onAssessment(slaAssessment(0.7, true, 2));   // 再次违约
        logAlerts("二次违约后");

        List<Alert> all = manager.allAlerts();
        assertEquals(2, all.size(), "恢复后再次违约应形成新的告警生命周期");
        assertEquals(AlertStatus.RESOLVED, all.get(0).status());
        assertEquals(AlertStatus.FIRING, all.get(1).status());
        assertNotEquals(all.get(0).alertId(), all.get(1).alertId());
        assertEquals(1, manager.activeAlerts().size());
        assertEquals(MetricType.SLA_ATTAINMENT, all.get(1).metricType());
    }
}
