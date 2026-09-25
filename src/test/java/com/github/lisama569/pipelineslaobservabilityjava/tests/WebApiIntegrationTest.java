package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端接口测试：配置的流水线在启动时注册，上报后立即可查询最新结论。
 */
@SpringBootTest(properties = "server.port=0")
@AutoConfigureMockMvc
class WebApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
    private final ObjectMapper json = new ObjectMapper();

    @Test
    @DisplayName("端到端：列出流水线 -> 上报 -> 重复上报幂等 -> 查询最新评估与告警")
    void reportAndQueryEndToEnd() throws Exception {
        mockMvc.perform(get("/api/v1/pipelines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].pipelineId").value("orders-hourly"));

        Instant now = Instant.now();
        String body = json.writeValueAsString(java.util.Map.of(
                "pipelineId", "orders-hourly",
                "runId", "it-run-1",
                "eventId", "it-e-1",
                "eventType", "STARTED",
                "eventTime", now.toString(),
                "scheduledTime", now.minusSeconds(5).toString()));
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"))
                .andExpect(jsonPath("$.ingestSeq").isNumber());

        // 重复上报 -> DUPLICATE
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("DUPLICATE"));

        // 未注册流水线 -> REJECTED/UNREGISTERED_PIPELINE（HTTP 仍 200，语义在体内）
        String unknown = json.writeValueAsString(java.util.Map.of(
                "pipelineId", "no-such-pipeline",
                "runId", "r", "eventId", "e",
                "eventType", "STARTED", "eventTime", now.toString()));
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(unknown))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("UNREGISTERED_PIPELINE"));

        // 终态事件
        String terminal = json.writeValueAsString(java.util.Map.of(
                "pipelineId", "orders-hourly",
                "runId", "it-run-1",
                "eventId", "it-e-2",
                "eventType", "SUCCEEDED",
                "eventTime", now.plusSeconds(10).toString()));
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(terminal))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"));

        mockMvc.perform(get("/api/v1/pipelines/orders-hourly/latest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.slaStatus").isNotEmpty())
                .andExpect(jsonPath("$.metrics.length()").value(3));

        mockMvc.perform(get("/api/v1/pipelines/orders-hourly/history?limit=2"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/pipelines/orders-hourly/alerts/active"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/pipelines/alerts/active"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("运行时注册流水线后即可上报；查询不存在的流水线返回 404")
    void runtimeRegisterAndNotFound() throws Exception {
        String def = """
                {
                  "id": "dynamic-pipeline",
                  "displayName": "动态流水线",
                  "windowSeconds": 120,
                  "retentionSeconds": 600,
                  "futureToleranceSeconds": 10,
                  "thresholds": {
                    "maxFreshnessSeconds": 60,
                    "maxScheduleDelaySeconds": 30,
                    "maxFailureRatePercent": 0,
                    "escalationDelaySeconds": 120,
                    "recoveryDelaySeconds": 30
                  }
                }
                """;
        mockMvc.perform(post("/api/v1/admin/pipelines")
                        .contentType(MediaType.APPLICATION_JSON).content(def))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pipelineId").value("dynamic-pipeline"));

        String event = json.writeValueAsString(java.util.Map.of(
                "pipelineId", "dynamic-pipeline",
                "runId", "r", "eventId", "e",
                "eventType", "STARTED", "eventTime", Instant.now().toString()));
        mockMvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(event))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"));

        mockMvc.perform(get("/api/v1/pipelines/missing/latest"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("手动触发评估（含历史 asOf 重放）返回可复现结论")
    void evaluateNowAndReplay() throws Exception {
        String resp = mockMvc.perform(post("/api/v1/pipelines/payments-5min/evaluate"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode node = json.readTree(resp);
        String asOf = node.get("asOf").asText();

        // 同一 asOf 重放，结论一致
        mockMvc.perform(post("/api/v1/pipelines/payments-5min/evaluate")
                        .param("asOf", asOf))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.asOf").value(asOf))
                .andExpect(jsonPath("$.slaStatus").value(node.get("slaStatus").asText()));
    }
}
