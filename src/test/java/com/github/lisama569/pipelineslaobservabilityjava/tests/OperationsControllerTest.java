package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.model.AckRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.AckStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.RuleUpdateStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 端到端：维护静默、告警认领、在线改规则三块能力的 HTTP 链路。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
class OperationsControllerTest {

    @Autowired
    private TestRestTemplate rest;

    private Map<String, Object> event(String pipeline, String run, String type,
                                      String status, String scheduled, String occurred) {
        var m = new LinkedHashMap<String, Object>();
        m.put("pipelineId", pipeline);
        m.put("runId", run);
        m.put("eventType", type);
        if (status != null) {
            m.put("status", status);
        }
        m.put("scheduledAt", scheduled);
        m.put("occurredAt", occurred);
        return m;
    }

    private void postEvent(Map<String, Object> e) {
        ResponseEntity<IngestResult> r = rest.postForEntity("/api/events", e, IngestResult.class);
        System.out.println("[OpsTest] 上报 -> " + r.getStatusCode() + " " + r.getBody());
    }

    @Test
    void maintenanceDeclareQueryRevokeOverHttp() {
        var req = new LinkedHashMap<String, Object>();
        req.put("pipelineId", "ops-maint");
        req.put("start", "2026-10-07T01:00:00Z");
        req.put("end", "2026-10-07T02:00:00Z");
        req.put("note", "计划换版");

        ResponseEntity<MaintenanceResult> declared =
                rest.postForEntity("/api/maintenance", req, MaintenanceResult.class);
        System.out.println("[OpsTest] 声明维护 -> " + declared.getStatusCode() + " " + declared.getBody());
        assertEquals(HttpStatus.OK, declared.getStatusCode());
        assertEquals(MaintenanceStatus.DECLARED, declared.getBody().status());

        // 重复声明幂等
        ResponseEntity<MaintenanceResult> dup =
                rest.postForEntity("/api/maintenance", req, MaintenanceResult.class);
        System.out.println("[OpsTest] 重复声明 -> " + dup.getBody());
        assertEquals(MaintenanceStatus.DUPLICATE, dup.getBody().status());

        MaintenanceWindow[] windows =
                rest.getForObject("/api/maintenance?pipelineId=ops-maint", MaintenanceWindow[].class);
        assertEquals(1, windows.length, "重复声明不得产生第二份记录");

        // 非法时间段：422 + 可区分原因
        var bad = new LinkedHashMap<String, Object>(req);
        bad.put("end", "2026-10-07T00:30:00Z");
        ResponseEntity<MaintenanceResult> rejected =
                rest.postForEntity("/api/maintenance", bad, MaintenanceResult.class);
        System.out.println("[OpsTest] 非法窗口 -> " + rejected.getStatusCode() + " " + rejected.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rejected.getStatusCode());
        assertEquals(MaintenanceRejectReason.INVALID_TIME_RANGE, rejected.getBody().reason());

        // 撤销 + 重复撤销
        ResponseEntity<MaintenanceResult> revoked =
                rest.postForEntity("/api/maintenance/revoke", req, MaintenanceResult.class);
        System.out.println("[OpsTest] 撤销 -> " + revoked.getBody());
        assertEquals(MaintenanceStatus.REVOKED, revoked.getBody().status());
        ResponseEntity<MaintenanceResult> again =
                rest.postForEntity("/api/maintenance/revoke", req, MaintenanceResult.class);
        assertEquals(HttpStatus.NOT_FOUND, again.getStatusCode());
    }

    @Test
    void maintenanceSuppressesAlertingOverHttp() {
        // 维护窗口覆盖 [01:00, 02:00)
        var req = new LinkedHashMap<String, Object>();
        req.put("pipelineId", "ops-silence");
        req.put("start", "2026-10-07T01:00:00Z");
        req.put("end", "2026-10-07T02:00:00Z");
        rest.postForEntity("/api/maintenance", req, MaintenanceResult.class);

        // 窗口内一次 FAILED：评估仍违约（可查），但不产生告警
        postEvent(event("ops-silence", "r1", "FINISHED", "FAILED",
                "2026-10-07T01:10:00Z", "2026-10-07T01:10:30Z"));
        Alert[] during = rest.getForObject("/api/alerts", Alert[].class);
        long duringCount = java.util.Arrays.stream(during)
                .filter(a -> a.pipelineId().equals("ops-silence")).count();
        System.out.println("[OpsTest] 窗口内活动告警数=" + duringCount);
        assertEquals(0, duringCount, "维护窗口内不得新拉告警");

        // 窗口结束后又一次 FAILED：必须触发新告警
        postEvent(event("ops-silence", "r2", "FINISHED", "FAILED",
                "2026-10-07T02:05:00Z", "2026-10-07T02:05:30Z"));
        Alert[] after = rest.getForObject("/api/alerts", Alert[].class);
        long afterCount = java.util.Arrays.stream(after)
                .filter(a -> a.pipelineId().equals("ops-silence")).count();
        System.out.println("[OpsTest] 窗口后活动告警数=" + afterCount);
        assertTrue(afterCount >= 1, "维护结束后仍违约必须触发新告警");
    }

    @Test
    void ackLifecycleOverHttp() {
        // 触发一条告警
        postEvent(event("ops-ack", "r1", "FINISHED", "FAILED",
                "2026-10-07T03:00:00Z", "2026-10-07T03:00:30Z"));
        Alert[] alerts = rest.getForObject("/api/alerts", Alert[].class);
        Alert target = java.util.Arrays.stream(alerts)
                .filter(a -> a.pipelineId().equals("ops-ack")).findFirst().orElseThrow();
        System.out.println("[OpsTest] 待认领告警=" + target);

        var ackReq = Map.of("ackBy", "lisa", "note", "值班处理中");
        ResponseEntity<AckResult> acked = rest.postForEntity(
                "/api/alerts/" + target.alertId() + "/ack", ackReq, AckResult.class);
        System.out.println("[OpsTest] 认领 -> " + acked.getStatusCode() + " " + acked.getBody());
        assertEquals(HttpStatus.OK, acked.getStatusCode());
        assertEquals(AckStatus.ACKNOWLEDGED, acked.getBody().status());
        assertEquals("lisa", acked.getBody().alert().acknowledgedBy());

        // 重复认领幂等
        ResponseEntity<AckResult> dup = rest.postForEntity(
                "/api/alerts/" + target.alertId() + "/ack", Map.of("ackBy", "bob"), AckResult.class);
        System.out.println("[OpsTest] 重复认领 -> " + dup.getBody());
        assertEquals(AckStatus.DUPLICATE, dup.getBody().status());
        assertEquals("lisa", dup.getBody().alert().acknowledgedBy());

        // 认领不存在的告警：422 + ALERT_NOT_FOUND
        ResponseEntity<AckResult> missing = rest.postForEntity(
                "/api/alerts/no-such/ack", ackReq, AckResult.class);
        System.out.println("[OpsTest] 认领不存在 -> " + missing.getStatusCode() + " " + missing.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, missing.getStatusCode());
        assertEquals(AckRejectReason.ALERT_NOT_FOUND, missing.getBody().reason());
    }

    @Test
    void ruleUpdateOverHttp() {
        var update = new LinkedHashMap<String, Object>();
        update.put("maxFreshnessLag", "PT5M");
        update.put("minSlaAttainment", 0.5);

        ResponseEntity<RuleUpdateResult> applied = rest.exchange(
                "/api/rules/ops-rule", HttpMethod.PUT, new HttpEntity<>(update), RuleUpdateResult.class);
        System.out.println("[OpsTest] 在线改规则 -> " + applied.getStatusCode() + " " + applied.getBody());
        assertEquals(HttpStatus.OK, applied.getStatusCode());
        assertEquals(RuleUpdateStatus.APPLIED, applied.getBody().status());

        String rule = rest.getForObject("/api/rules/ops-rule", String.class);
        System.out.println("[OpsTest] 生效规则=" + rule);
        assertTrue(rule.contains("PT5M") || rule.contains("300"), "新阈值应立即可查");

        // 非法更新：422 + 可区分原因，且不影响已生效规则
        var bad = Map.of("recoveryMargin", 1.5);
        ResponseEntity<RuleUpdateResult> rejected = rest.exchange(
                "/api/rules/ops-rule", HttpMethod.PUT, new HttpEntity<>(bad), RuleUpdateResult.class);
        System.out.println("[OpsTest] 非法更新 -> " + rejected.getStatusCode() + " " + rejected.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rejected.getStatusCode());
        assertEquals(RuleRejectReason.INVALID_RECOVERY_MARGIN, rejected.getBody().reason());
        String after = rest.getForObject("/api/rules/ops-rule", String.class);
        assertEquals(rule, after, "校验失败不得改动已生效规则");
    }
}
