package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckResult;
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

    private AlertManager manager;

    @BeforeEach
    void setUp() {
        SlaProperties props = new SlaProperties();
        SlaProperties.PipelineRule rule = props.getDefaultRule();
        rule.setMinSlaAttainment(0.9);
        rule.setRecoveryMargin(0.05);
        rule.setEscalateAfter(3);
        manager = new AlertManager(props);
    }

    private PipelineAssessment slaAssessment(double attainment, boolean breached, int minute) {
        Instant at = T0.plus(Duration.ofMinutes(minute));
        var a = new PipelineAssessment("p1", at, at.minus(Duration.ofHours(1)),
                false, 0L, 0L, false, attainment, breached, List.of());
        System.out.println("[AckTest] 评估: attainment=" + attainment + " breached=" + breached + " at=" + at);
        return a;
    }

    private String fireOne() {
        manager.onAssessment(slaAssessment(0.8, true, 0));
        String id = manager.activeAlerts().get(0).alertId();
        System.out.println("[AckTest] 触发告警: " + id);
        return id;
    }

    @Test
    void ackAttachesInfoAndSurvivesEscalationAndResolution() {
        String id = fireOne();
        Instant ackAt = T0.plus(Duration.ofMinutes(1));
        AckResult ack = manager.ack(id, "lisa", "正在处理", ackAt);
        System.out.println("[AckTest] 认领 -> " + ack);
        assertEquals(AckStatus.ACKNOWLEDGED, ack.status());
        assertEquals("lisa", ack.alert().acknowledgedBy());
        assertEquals("正在处理", ack.alert().ackNote());
        assertEquals(ackAt, ack.alert().acknowledgedAt());

        // 认领后继续违约：不开新告警、状态不倒退，升级时认领信息保留
        manager.onAssessment(slaAssessment(0.8, true, 2));
        manager.onAssessment(slaAssessment(0.8, true, 3)); // 第 3 次连续违约 → 升级
        Alert escalated = manager.activeAlerts().get(0);
        System.out.println("[AckTest] 升级后 -> " + escalated);
        assertEquals(1, manager.allAlerts().size(), "认领后继续违约不得新开告警");
        assertEquals(Severity.CRITICAL, escalated.severity());
        assertEquals("lisa", escalated.acknowledgedBy(), "升级不得丢失认领信息");
        assertEquals("正在处理", escalated.ackNote());

        // 恢复后认领信息仍挂在该告警的历史快照上
        manager.onAssessment(slaAssessment(0.99, false, 4));
        Alert resolved = manager.allAlerts().get(0);
        System.out.println("[AckTest] 恢复后 -> " + resolved);
        assertEquals(AlertStatus.RESOLVED, resolved.status());
        assertEquals("lisa", resolved.acknowledgedBy(), "恢复快照上认领信息应保留");
    }

    @Test
    void duplicateAckIsIdempotent() {
        String id = fireOne();
        manager.ack(id, "lisa", "首次", T0.plus(Duration.ofMinutes(1)));
        AckResult dup = manager.ack(id, "bob", "覆盖尝试", T0.plus(Duration.ofMinutes(2)));
        System.out.println("[AckTest] 重复认领 -> " + dup);
        assertEquals(AckStatus.DUPLICATE, dup.status(), "重复认领应幂等而非报错");
        assertEquals("lisa", dup.alert().acknowledgedBy(), "重复认领不得覆盖既有认领人");
        assertEquals("首次", dup.alert().ackNote());
        assertEquals(T0.plus(Duration.ofMinutes(1)), dup.alert().acknowledgedAt());
    }

    @Test
    void ackOnResolvedOrUnknownAlertIsRejectedWithDistinctReasons() {
        String id = fireOne();
        manager.onAssessment(slaAssessment(0.99, false, 1)); // 恢复

        AckResult resolved = manager.ack(id, "lisa", null, T0.plus(Duration.ofMinutes(2)));
        System.out.println("[AckTest] 认领已恢复告警 -> " + resolved);
        assertEquals(AckStatus.REJECTED, resolved.status());
        assertEquals(AckRejectReason.ALERT_ALREADY_RESOLVED, resolved.reason(),
                "已恢复告警的拒绝原因必须可区分");

        AckResult unknown = manager.ack("no-such-alert", "lisa", null, T0.plus(Duration.ofMinutes(2)));
        System.out.println("[AckTest] 认领不存在告警 -> " + unknown);
        assertEquals(AckStatus.REJECTED, unknown.status());
        assertEquals(AckRejectReason.ALERT_NOT_FOUND, unknown.reason());

        AckResult blank = manager.ack(id, " ", null, T0.plus(Duration.ofMinutes(2)));
        System.out.println("[AckTest] 缺认领人 -> " + blank);
        assertEquals(AckStatus.REJECTED, blank.status());
        assertEquals(AckRejectReason.MISSING_ACK_BY, blank.reason());
    }

    @Test
    void newIncidentCycleAfterResolveStartsUnacknowledged() {
        String first = fireOne();
        manager.ack(first, "lisa", "第一轮", T0.plus(Duration.ofMinutes(1)));
        manager.onAssessment(slaAssessment(0.99, false, 2)); // 恢复
        manager.onAssessment(slaAssessment(0.7, true, 3));   // 再次违约 → 新生命周期

        List<Alert> all = manager.allAlerts();
        System.out.println("[AckTest] 二次违约后 -> " + all);
        assertEquals(2, all.size());
        Alert second = all.get(1);
        assertNotEquals(first, second.alertId());
        assertNull(second.acknowledgedBy(), "新事故周期的告警默认未认领");
        assertNull(second.ackNote());
        assertNull(second.acknowledgedAt());

        // 新告警可以独立认领
        AckResult ack = manager.ack(second.alertId(), "bob", "第二轮", T0.plus(Duration.ofMinutes(4)));
        assertEquals(AckStatus.ACKNOWLEDGED, ack.status());
        assertEquals("bob", manager.findAlert(second.alertId()).acknowledgedBy());
        assertEquals("lisa", manager.findAlert(first).acknowledgedBy(), "旧告警认领信息不受影响");
    }
}
