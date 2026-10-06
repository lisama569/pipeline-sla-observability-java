package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.model.AckResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.AlertStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateStatus;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 端到端：维护静默、告警认领、在线改规则三个新能力的 HTTP 链路。
 * 事件时间统一取 2026-10-07 当天且按方法间递增，避免共享水位线导致超保留期拒绝；
 * 本类结束后销毁 Spring 上下文，不影响其他测试类。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OpsEndpointsTest {

    @Autowired
    private TestRestTemplate rest;

    private Map<String, Object> event(String pipeline, String run, String status, String occurred) {
        var m = new LinkedHashMap<String, Object>();
        m.put("pipelineId", pipeline);
        m.put("runId", run);
        m.put("eventType", "FINISHED");
        m.put("status", status);
        m.put("scheduledAt", occurred);
        m.put("occurredAt", occurred);
        return m;
    }

    private void ingest(String pipeline, String run, String status, String occurred) {
        ResponseEntity<IngestResult> r = rest.postForEntity("/api/events",
                event(pipeline, run, status, occurred), IngestResult.class);
        System.out.println("[OpsTest] 上报 " + pipeline + "/" + run + " -> " + r.getBody());
        assertEquals(IngestStatus.ACCEPTED, r.getBody().status());
    }

    private List<Alert> alertsOf(String pipeline, boolean includeResolved) {
        Alert[] all = rest.getForObject("/api/alerts?includeResolved=" + includeResolved, Alert[].class);
        return Arrays.stream(all).filter(a -> a.pipelineId().equals(pipeline)).toList();
    }

    @Test
    @Order(1)
    void maintenanceFlowOverHttp() {
        var window = new LinkedHashMap<String, Object>();
        window.put("windowId", "mw-ops-1");
        window.put("pipelineId", "ops-mw");
        window.put("metricType", "SLA_ATTAINMENT");
        window.put("start", "2026-10-07T00:00:00Z");
        window.put("end", "2026-10-07T02:00:00Z");
        window.put("reason", "版本发布");

        ResponseEntity<MaintenanceResult> declared = rest.postForEntity("/api/maintenance", window, MaintenanceResult.class);
        System.out.println("[OpsTest] 声明维护 -> " + declared.getStatusCode() + " " + declared.getBody());
        assertEquals(HttpStatus.OK, declared.getStatusCode());
        assertEquals(MaintenanceStatus.DECLARED, declared.getBody().status());

        ResponseEntity<MaintenanceResult> dup = rest.postForEntity("/api/maintenance", window, MaintenanceResult.class);
        System.out.println("[OpsTest] 重复声明 -> " + dup.getBody());
        assertEquals(MaintenanceStatus.DUPLICATE, dup.getBody().status(), "重复声明幂等");

        var conflict = new LinkedHashMap<>(window);
        conflict.put("end", "2026-10-07T03:00:00Z");
        ResponseEntity<MaintenanceResult> conflicting = rest.postForEntity("/api/maintenance", conflict, MaintenanceResult.class);
        System.out.println("[OpsTest] 同 id 不同内容 -> " + conflicting.getStatusCode() + " " + conflicting.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, conflicting.getStatusCode());
        assertEquals(MaintenanceRejectReason.CONFLICTING_WINDOW, conflicting.getBody().reason());

        MaintenanceWindow[] listed = rest.getForObject("/api/maintenance", MaintenanceWindow[].class);
        System.out.println("[OpsTest] 窗口列表=" + Arrays.toString(listed));
        assertEquals(1, Arrays.stream(listed).filter(w -> w.windowId().equals("mw-ops-1")).count(),
                "重复/冲突声明不得产生额外记录");

        // 窗口内违约：评估照常违约，SLA 告警被按住；指标级窗口不覆盖 FRESHNESS，故新鲜度告警照常触发
        ingest("ops-mw", "r1", "FAILED", "2026-10-07T00:30:00Z");
        PipelineAssessment a = rest.getForObject("/api/pipelines/ops-mw/assessment", PipelineAssessment.class);
        System.out.println("[OpsTest] 窗口内评估=" + a);
        assertTrue(a.slaBreached(), "维护窗口内评估结论照常可查");
        List<Alert> during = alertsOf("ops-mw", true);
        System.out.println("[OpsTest] 窗口内告警=" + during);
        assertTrue(during.stream().noneMatch(al -> al.metricType() == MetricType.SLA_ATTAINMENT),
                "窗口内不得新拉被静默指标的告警");
        assertTrue(during.stream().anyMatch(al -> al.metricType() == MetricType.FRESHNESS),
                "指标级窗口不得按住其他指标的告警");

        // 窗口结束后仍违约：必须形成新告警
        ingest("ops-mw", "r2", "FAILED", "2026-10-07T02:30:00Z");
        List<Alert> after = alertsOf("ops-mw", false);
        System.out.println("[OpsTest] 窗口结束后告警=" + after);
        assertTrue(after.stream().anyMatch(al -> al.metricType() == MetricType.SLA_ATTAINMENT),
                "窗口过后仍违约必须形成新告警");

        ResponseEntity<MaintenanceResult> revoked = rest.exchange("/api/maintenance/mw-ops-1",
                HttpMethod.DELETE, HttpEntity.EMPTY, MaintenanceResult.class);
        System.out.println("[OpsTest] 撤销 -> " + revoked.getBody());
        assertEquals(MaintenanceStatus.REVOKED, revoked.getBody().status());
        ResponseEntity<MaintenanceResult> again = rest.exchange("/api/maintenance/mw-ops-1",
                HttpMethod.DELETE, HttpEntity.EMPTY, MaintenanceResult.class);
        assertEquals(HttpStatus.NOT_FOUND, again.getStatusCode());
        assertEquals(MaintenanceStatus.NOT_FOUND, again.getBody().status(), "重复撤销原因可区分");
    }

    @Test
    @Order(2)
    void ackFlowOverHttp() {
        ingest("ops-ack", "r1", "FAILED", "2026-10-07T03:00:00Z");
        Alert sla = alertsOf("ops-ack", false).stream()
                .filter(al -> al.metricType() == MetricType.SLA_ATTAINMENT).findFirst().orElseThrow();
        System.out.println("[OpsTest] 待认领告警=" + sla);

        var ackBody = new LinkedHashMap<String, Object>();
        ackBody.put("operator", "lisa");
        ackBody.put("note", "值班处理中");
        ResponseEntity<AckResult> acked = rest.postForEntity("/api/alerts/" + sla.alertId() + "/ack", ackBody, AckResult.class);
        System.out.println("[OpsTest] 认领 -> " + acked.getStatusCode() + " " + acked.getBody());
        assertEquals(HttpStatus.OK, acked.getStatusCode());
        assertEquals(AckStatus.ACKNOWLEDGED, acked.getBody().status());
        assertEquals("lisa", acked.getBody().alert().acknowledgedBy());

        var ackAgain = new LinkedHashMap<String, Object>();
        ackAgain.put("operator", "bob");
        ResponseEntity<AckResult> dup = rest.postForEntity("/api/alerts/" + sla.alertId() + "/ack", ackAgain, AckResult.class);
        System.out.println("[OpsTest] 重复认领 -> " + dup.getBody());
        assertEquals(AckStatus.ALREADY_ACKNOWLEDGED, dup.getBody().status(), "重复认领幂等不报错");
        assertEquals("lisa", dup.getBody().alert().acknowledgedBy(), "重复认领不得覆盖原认领人");

        ResponseEntity<AckResult> notFound = rest.postForEntity("/api/alerts/no-such-alert/ack", ackBody, AckResult.class);
        System.out.println("[OpsTest] 认领不存在 -> " + notFound.getStatusCode() + " " + notFound.getBody());
        assertEquals(HttpStatus.NOT_FOUND, notFound.getStatusCode());
        assertEquals(AckStatus.ALERT_NOT_FOUND, notFound.getBody().status());

        // 认领后继续违约不新开告警；恢复健康后告警自动恢复
        ingest("ops-ack", "r2", "SUCCESS", "2026-10-07T03:40:00Z");
        ingest("ops-ack", "r3", "SUCCESS", "2026-10-07T03:50:00Z");
        ingest("ops-ack", "r4", "SUCCESS", "2026-10-07T04:00:00Z");
        ingest("ops-ack", "r5", "SUCCESS", "2026-10-07T04:10:00Z");
        List<Alert> all = alertsOf("ops-ack", true);
        System.out.println("[OpsTest] 恢复后告警=" + all);
        Alert resolved = all.stream().filter(al -> al.alertId().equals(sla.alertId())).findFirst().orElseThrow();
        assertEquals(AlertStatus.RESOLVED, resolved.status());
        assertEquals("lisa", resolved.acknowledgedBy(), "认领信息随生命周期保留");
        assertEquals(1, all.stream().filter(al -> al.metricType() == MetricType.SLA_ATTAINMENT).count(),
                "认领后持续违约不得新开告警");

        ResponseEntity<AckResult> late = rest.postForEntity("/api/alerts/" + sla.alertId() + "/ack", ackBody, AckResult.class);
        System.out.println("[OpsTest] 已恢复认领 -> " + late.getStatusCode() + " " + late.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, late.getStatusCode());
        assertEquals(AckStatus.ALERT_ALREADY_RESOLVED, late.getBody().status(),
                "已恢复拒绝与不存在必须可区分");
    }

    @Test
    @Order(3)
    void ruleUpdateFlowOverHttp() {
        var update = new LinkedHashMap<String, Object>();
        update.put("minSlaAttainment", 0.5);
        ResponseEntity<RuleUpdateResult> applied = rest.exchange("/api/rules/ops-rule",
                HttpMethod.PUT, new HttpEntity<>(update), RuleUpdateResult.class);
        System.out.println("[OpsTest] 在线改规则 -> " + applied.getStatusCode() + " " + applied.getBody());
        assertEquals(HttpStatus.OK, applied.getStatusCode());
        assertEquals(RuleUpdateStatus.APPLIED, applied.getBody().status());
        assertEquals(0.5, applied.getBody().effectiveRule().getMinSlaAttainment());

        Map<?, ?> rule = rest.getForObject("/api/rules/ops-rule", Map.class);
        System.out.println("[OpsTest] 生效规则=" + rule);
        assertEquals(0.5, ((Number) rule.get("minSlaAttainment")).doubleValue(), "改完立即对后续查询生效");

        // 非法入参：422 + 可区分原因 + 整条不生效（合法字段也不落盘）
        var mixed = new LinkedHashMap<String, Object>();
        mixed.put("window", "PT0S");
        mixed.put("minSlaAttainment", 0.4);
        ResponseEntity<RuleUpdateResult> rejected = rest.exchange("/api/rules/ops-rule",
                HttpMethod.PUT, new HttpEntity<>(mixed), RuleUpdateResult.class);
        System.out.println("[OpsTest] 非法更新 -> " + rejected.getStatusCode() + " " + rejected.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rejected.getStatusCode());
        assertEquals(RuleRejectReason.INVALID_WINDOW, rejected.getBody().reason());
        Map<?, ?> after = rest.getForObject("/api/rules/ops-rule", Map.class);
        assertEquals(0.5, ((Number) after.get("minSlaAttainment")).doubleValue(),
                "校验不过整条不生效，合法字段也不得部分落盘");

        var badEscalate = new LinkedHashMap<String, Object>();
        badEscalate.put("escalateAfter", 0);
        ResponseEntity<RuleUpdateResult> r2 = rest.exchange("/api/rules/ops-rule",
                HttpMethod.PUT, new HttpEntity<>(badEscalate), RuleUpdateResult.class);
        assertEquals(RuleRejectReason.INVALID_ESCALATE_AFTER, r2.getBody().reason(),
                "升级次数小于 1 的原因必须可区分");

        // 新阈值立即作用于后续上报：0.75 >= 0.5 不再违约
        ingest("ops-rule", "ok1", "SUCCESS", "2026-10-07T05:00:00Z");
        ingest("ops-rule", "ok2", "SUCCESS", "2026-10-07T05:10:00Z");
        ingest("ops-rule", "ok3", "SUCCESS", "2026-10-07T05:20:00Z");
        ingest("ops-rule", "bad1", "FAILED", "2026-10-07T05:30:00Z");
        PipelineAssessment a = rest.getForObject("/api/pipelines/ops-rule/assessment", PipelineAssessment.class);
        System.out.println("[OpsTest] 改后评估=" + a);
        assertEquals(0.75, a.slaAttainment());
        assertFalse(a.slaBreached(), "新阈值 0.5 下 0.75 不再违约，在线修改立即生效");
    }
}
