package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.Severity;
import com.github.lisama569.pipelineslaobservabilityjava.service.AlertManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AlertAckTest {

    private static final Instant T0 = Instant.parse("2026-10-07T00:00:00Z");
    private static final Instant ACK_AT = Instant.parse("2026-10-07T01:00:00Z");

    private AlertManager manager;

    @BeforeEach
    void setUp() {
        SlaProperties props = new SlaProperties();
        props.getDefaultRule().setMinSlaAttainment(0.9);
        props.getDefaultRule().setRecoveryMargin(0.05);
        props.getDefaultRule().setEscalateAfter(3);
        manager = new AlertManager(props);
    }

    private PipelineAssessment slaAssessment(double attainment, boolean breached, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment("p1", at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, attainment, breached, List.of());
        System.out.println("[AckTest] 评估: attainment=" + attainment + " breached=" + breached + " at=" + at);
        return a;
    }

    private void breach(int minute) {
        manager.onAssessment(slaAssessment(0.8, true, minute));
    }

    @Test
    void ackPersistsThroughEscalationAndResolve() {
        breach(0);
        String alertId = manager.activeAlerts().get(0).alertId();

        var ack = manager.acknowledge(alertId, "lisa", "跟进中", ACK_AT);
        System.out.println("[AckTest] 认领 -> " + ack);
        assertEquals(AckStatus.ACKNOWLEDGED, ack.status());
        assertEquals("lisa", ack.alert().acknowledgedBy());
        assertEquals("跟进中", ack.alert().ackNote());
        assertEquals(ACK_AT, ack.alert().acknowledgedAt());

        breach(1);
        breach(2); // 第 3 次连续违约 → 升级
        Alert escalated = manager.activeAlerts().get(0);
        System.out.println("[AckTest] 升级后告警=" + escalated);
        assertEquals(Severity.CRITICAL, escalated.severity());
        assertEquals(alertId, escalated.alertId(), "认领后继续违约不得新开告警");
        assertEquals("lisa", escalated.acknowledgedBy(), "升级不得丢失认领信息");
        assertEquals("跟进中", escalated.ackNote());

        manager.onAssessment(slaAssessment(0.99, false, 3)); // 带裕量恢复
        Alert resolved = manager.allAlerts().get(0);
        System.out.println("[AckTest] 恢复后告警=" + resolved);
        assertEquals(AlertStatus.RESOLVED, resolved.status());
        assertEquals("lisa", resolved.acknowledgedBy(), "认领信息挂到本次生命周期结束");
    }

    @Test
    void repeatedAckIsIdempotentWithoutExtraChange() {
        breach(0);
        String alertId = manager.activeAlerts().get(0).alertId();
        manager.acknowledge(alertId, "lisa", "首次认领", ACK_AT);

        var again = manager.acknowledge(alertId, "bob", "重复认领", ACK_AT.plusSeconds(60));
        System.out.println("[AckTest] 重复认领 -> " + again);
        assertEquals(AckStatus.ALREADY_ACKNOWLEDGED, again.status(), "重复认领要幂等、不报错");
        Alert alert = manager.allAlerts().get(0);
        assertEquals("lisa", alert.acknowledgedBy(), "重复认领不得覆盖原认领人");
        assertEquals("首次认领", alert.ackNote(), "重复认领不得覆盖原备注");
        assertEquals(ACK_AT, alert.acknowledgedAt(), "重复认领不得改写认领时间");
        assertEquals(1, manager.allAlerts().size(), "重复认领不得产生额外变化");
    }

    @Test
    void ackOnResolvedAlertIsRejectedWithDistinguishableReason() {
        breach(0);
        String alertId = manager.activeAlerts().get(0).alertId();
        manager.onAssessment(slaAssessment(0.99, false, 1)); // 恢复

        var rejected = manager.acknowledge(alertId, "lisa", "迟到认领", ACK_AT);
        System.out.println("[AckTest] 已恢复告警认领 -> " + rejected);
        assertEquals(AckStatus.ALERT_ALREADY_RESOLVED, rejected.status(), "已恢复告警必须明确拒绝");

        var notFound = manager.acknowledge("no-such-alert", "lisa", null, ACK_AT);
        System.out.println("[AckTest] 不存在告警认领 -> " + notFound);
        assertEquals(AckStatus.ALERT_NOT_FOUND, notFound.status());
        assertNotEquals(rejected.status(), notFound.status(), "已恢复与不存在必须可区分");
    }

    @Test
    void newLifecycleAfterResolveStartsUnacknowledged() {
        breach(0);
        String firstId = manager.activeAlerts().get(0).alertId();
        manager.acknowledge(firstId, "lisa", "第一次事故", ACK_AT);
        breach(1);
        breach(2);
        assertEquals(1, manager.allAlerts().size(), "认领后持续违约不得新开告警");

        manager.onAssessment(slaAssessment(0.99, false, 3)); // 恢复
        breach(4); // 再次违约 → 新生命周期
        List<Alert> all = manager.allAlerts();
        System.out.println("[AckTest] 二次违约后全部告警=" + all);
        assertEquals(2, all.size(), "恢复后再违约应形成全新告警");
        Alert second = all.get(1);
        assertNotEquals(firstId, second.alertId());
        assertFalse(second.acknowledged(), "新告警默认未认领，认领状态重新算");
        assertNull(second.acknowledgedBy());
        assertEquals("lisa", all.get(0).acknowledgedBy(), "旧告警的认领信息保留在历史快照上");
    }
}
