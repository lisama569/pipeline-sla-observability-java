package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.model.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.PipelineAssessment;
import com.github.lisama569.pipelineslaobservabilityjava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 端到端：服务本地启动后，上报与查询链路可用，失败原因可区分。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObservabilityControllerTest {

    @Autowired
    private TestRestTemplate rest;

    private Map<String, Object> event(String pipeline, String run, String type,
                                      String status, String scheduled, String occurred) {
        var m = new java.util.LinkedHashMap<String, Object>();
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

    @Test
    void ingestEvaluateAndQueryOverHttp() {
        var started = event("orders", "r1", "STARTED", null,
                "2026-09-26T00:00:00Z", "2026-09-26T00:00:30Z");
        ResponseEntity<IngestResult> r1 = rest.postForEntity("/api/events", started, IngestResult.class);
        System.out.println("[RestTest] 上报 STARTED -> " + r1.getStatusCode() + " " + r1.getBody());
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        assertEquals(IngestStatus.ACCEPTED, r1.getBody().status());

        // 重复上报：幂等，返回 DUPLICATE 且无副作用
        ResponseEntity<IngestResult> dup = rest.postForEntity("/api/events", started, IngestResult.class);
        System.out.println("[RestTest] 重复上报 -> " + dup.getStatusCode() + " " + dup.getBody());
        assertEquals(IngestStatus.DUPLICATE, dup.getBody().status());

        // 失败完成 → 达标率 0 → 触发告警
        var failed = event("orders", "r1", "FINISHED", "FAILED",
                "2026-09-26T00:00:00Z", "2026-09-26T00:02:00Z");
        rest.postForEntity("/api/events", failed, IngestResult.class);

        PipelineAssessment a = rest.getForObject("/api/pipelines/orders/assessment", PipelineAssessment.class);
        System.out.println("[RestTest] 评估=" + a);
        assertEquals(0.0, a.slaAttainment());
        assertTrue(a.slaBreached());

        Alert[] alerts = rest.getForObject("/api/alerts", Alert[].class);
        System.out.println("[RestTest] 活动告警=" + java.util.Arrays.toString(alerts));
        assertTrue(alerts.length >= 1, "违约后应存在活动告警");

        // 非法事件：422 + 可区分原因
        var bad = event("orders", "r2", "FINISHED", null,
                "2026-09-26T00:00:00Z", "2026-09-26T00:01:00Z");
        ResponseEntity<IngestResult> r2 = rest.postForEntity("/api/events", bad, IngestResult.class);
        System.out.println("[RestTest] 非法事件 -> " + r2.getStatusCode() + " " + r2.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, r2.getStatusCode());
        assertEquals(RejectReason.MISSING_STATUS, r2.getBody().reason());
    }
}
