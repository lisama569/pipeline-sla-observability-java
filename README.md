# Pipeline SLA Observability Service

流水线可观测性服务：接收作业运行上报，评估各流水线的新鲜度 / 调度延迟 / SLA 达标状态，
并维护告警的完整生命周期。数据仅保存在进程内，可本地启动并对外提供 REST 上报与查询能力。

## 本地启动与验证

```bash
# 启动（默认 8080 端口，见 application.properties）
mvn spring-boot:run

# 上报事件
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "pipelineId":"orders","runId":"r1","eventType":"STARTED",
  "scheduledAt":"2026-09-26T00:00:00Z","occurredAt":"2026-09-26T00:00:30Z"}'
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "pipelineId":"orders","runId":"r1","eventType":"FINISHED","status":"SUCCESS",
  "scheduledAt":"2026-09-26T00:00:00Z","occurredAt":"2026-09-26T00:02:00Z"}'

# 查询评估结论 / 告警
curl localhost:8080/api/pipelines/orders/assessment
curl localhost:8080/api/pipelines/assessments
curl 'localhost:8080/api/alerts'                      # 仅活动告警
curl 'localhost:8080/api/alerts?includeResolved=true' # 含已恢复

# 计划内维护 / 静默：声明（幂等）→ 窗口内违约不拉告警 → 窗口过后仍违约会补一条新告警
curl -X POST localhost:8080/api/maintenance -H 'Content-Type: application/json' -d '{
  "windowId":"mw-001","pipelineId":"orders","metricType":null,
  "start":"2026-09-26T00:00:00Z","end":"2026-09-26T02:00:00Z","reason":"版本发布"}'
curl localhost:8080/api/maintenance                   # 查看已登记窗口
curl -X DELETE localhost:8080/api/maintenance/mw-001  # 撤销

# 告警认领（alertId 从 /api/alerts 返回中取；alertId 含 | 和 :，拼进 URL 前需做 URL 编码）
AID=$(curl -s localhost:8080/api/alerts | python3 -c \
  "import sys,json,urllib.parse; print(urllib.parse.quote(json.load(sys.stdin)[0]['alertId'], safe=''))")
curl -X POST "localhost:8080/api/alerts/$AID/ack" -H 'Content-Type: application/json' -d '{
  "operator":"lisa","note":"值班处理中"}'

# 在线改规则（立即生效，无需重启；字段可只给要改的项）
curl -X PUT localhost:8080/api/rules/orders -H 'Content-Type: application/json' -d '{
  "window":"PT30M","minSlaAttainment":0.8}'
curl localhost:8080/api/rules/orders                  # 查看当前生效规则

# 运行测试（单测日志会打印输入事件与判定依据）
mvn test
```

## 事件语义

事件（`JobRunEvent`）字段：`pipelineId`、`runId`、`eventType`(STARTED/FINISHED)、
`status`(SUCCESS/FAILED，仅 FINISHED 需要)、`scheduledAt`、`occurredAt`（事件时间）。

- **幂等去重**：去重键 = `pipelineId|runId|eventType`。
  - 同键同内容 → `DUPLICATE`，无副作用、不重复计数（HTTP 200）。
  - 同键不同内容 → 拒绝 `CONFLICTING_EVENT`（HTTP 422），不部分生效。
- **乱序 / 迟到**：事件按 `occurredAt` 归位，评估只依赖事件时间水位线
  （所有已接受事件 `occurredAt` 的最大值），不依赖到达顺序与墙钟，
  因此同一组事件任意顺序上报得到完全一致的评估结论（可复现）。
- **时钟回拨矛盾**：同一 `runId` 内 STARTED 晚于已接受的 FINISHED
  （或 FINISHED 早于已接受的 STARTED）→ 拒绝 `EVENT_TIME_REGRESSION`。
- **过迟到**：`occurredAt` 早于 `水位线 - retention` → 拒绝 `BEYOND_RETENTION`；
  已存事件超过保留期被清理，存储与评估开销有界（不随历史无界劣化）。
- **其他可区分拒绝原因**：`INVALID_EVENT`（必填字段缺失）、
  `MISSING_STATUS`（FINISHED 缺终态）、
  `UNKNOWN_PIPELINE`（开启 `sla.require-registered-pipelines` 后未注册的流水线）。

## 评估规则

评估窗口 = `[水位线 - window, 水位线]`，每条流水线可单独配置（`sla.pipelines.<id>.*`），
未配置时使用 `sla.default-rule.*`：

| 指标 | 定义 | 违约条件 |
| --- | --- | --- |
| 新鲜度 | 水位线 − 最近一次 SUCCESS 完成时间 | 滞后 > `max-freshness-lag`；窗口内有事件但从未成功完成亦违约 |
| 调度延迟 | 窗口内 STARTED 的 `occurredAt − scheduledAt` 最大值 | > `max-schedule-delay` |
| SLA 达标率 | 窗口内 FINISHED 中 SUCCESS 占比 | < `min-sla-attainment` |

评估结论（`PipelineAssessment`）携带 `contributingEventKeys`（参与评估的事件键），
后续事件引起的历史结论变化可追溯到具体事件。阈值边界为"恰好等于阈值不违约"。

## 告警生命周期

告警键 = `(pipelineId, metricType)`，状态机：`FIRING → RESOLVED`（自动恢复）。

- **触发**：指标违约且无活动告警 → 新建 WARNING 告警（时间取事件时间水位线）。
- **去重 / 滞回**：持续违约只更新既有告警；指标回到阈值附近
  （未越过 `阈值 ± recovery-margin`）时保持现状，抖动不会产生重复告警或反复恢复。
- **升级**：连续违约评估次数达到 `escalate-after` → WARNING 升级为 CRITICAL
  （`escalationCount` 递增，不产生新告警）。
- **自动恢复**：指标带裕量恢复健康 → 告警置为 RESOLVED 并记录 `resolvedAt`。
- **新生命周期**：恢复后再次违约生成新的 `alertId`。
- **并发安全**：`EventStore` / `AlertManager` 的状态迁移均在监视器锁内完成，
  并发上报与并发评估下不丢告警、不重复触发、状态不倒退。

## 计划内维护 / 静默

值班同学可对某条流水线（或它的某一个指标）声明维护时间段，按住这段时间内的违约告警。

- **声明**：`POST /api/maintenance`，body 为 `MaintenanceWindow`
  （`windowId`、`pipelineId`、`metricType`（null = 全部指标）、`start`、`end`、`reason`）。
  - `windowId` 是幂等键：同 id 同内容重复声明 → `DUPLICATE`（HTTP 200，无副作用）；
    同 id 不同内容 → 拒绝 `CONFLICTING_WINDOW`（HTTP 422）；
    字段缺失或 `start >= end` → 拒绝 `INVALID_WINDOW`（HTTP 422），原因可区分。
- **撤销**：`DELETE /api/maintenance/{windowId}`；撤销不存在的 id 返回 `NOT_FOUND`（HTTP 404）。
- **查询**：`GET /api/maintenance` 列出全部已登记窗口。
- **静默语义**：是否处于维护按**事件时间**口径判断（评估水位线落在 `[start, end)` 内），
  不读墙钟，同一批事件重放结论一致。落在窗口内的违约：
  - 评估结论照常可查（`GET /api/pipelines/{id}/assessment` 仍显示违约）；
  - 不新拉告警、既有告警不升级、连续违约计数暂停；
  - 恢复侧不受影响：窗口内指标带裕量恢复健康，告警照常自动恢复。
- **窗口结束**：`end` 为开区间，窗口一过若指标仍在违约，后续评估会正常形成一条**新告警**，
  不会因为静默过就当没事发生。
- 声明与撤销可并发进行，状态迁移在锁内完成，不互相覆盖、不丢记录；
  窗口按 `(pipelineId, metricType)` 作用域生效，一条流水线的静默不影响其他流水线。

## 告警认领

活动中的告警可被认领：`POST /api/alerts/{alertId}/ack`，
body 为 `{"operator":"lisa","note":"值班处理中"}`（`operator` 必填，缺失返回 400 `INVALID_REQUEST`）。

- 认领结果状态可区分：`ACKNOWLEDGED`（成功）/ `ALREADY_ACKNOWLEDGED`（幂等重复，HTTP 200，
  不覆盖原认领人与备注）/ `ALERT_NOT_FOUND`（HTTP 404）/
  `ALERT_ALREADY_RESOLVED`（已恢复，明确拒绝，HTTP 422）。
- 认领后该告警继续违约**不会**新开告警，后续评估也不会把认领状态倒回去；
  升级、恢复等快照变迁都携带认领信息，认领信息一直挂到本次生命周期结束。
- 恢复后再次违约属于新的事故周期：生成全新 `alertId`，新告警默认未认领，认领状态重新算。

## 在线规则更新

`PUT /api/rules/{pipelineId}` 可在线修改该流水线的评估规则，无需重启；
body 为 `RuleUpdateRequest`，所有字段可空，null 表示该项保持不变
（`window`、`maxFreshnessLag`、`maxScheduleDelay`、`minSlaAttainment`、`recoveryMargin`、`escalateAfter`）。
`GET /api/rules/{pipelineId}` 查看当前生效规则。

- **立即生效**：改完对后续上报与查询立刻生效；同一份事件在未再变更规则时结论稳定一致。
- **全量前置校验**：任一字段非法则整条拒绝（HTTP 422），合法字段也不会部分落盘。
  拒绝原因可区分：`INVALID_WINDOW`（窗口非正数）、`INVALID_MAX_FRESHNESS_LAG` /
  `INVALID_MAX_SCHEDULE_DELAY`（阈值为负）、`INVALID_MIN_SLA_ATTAINMENT` /
  `INVALID_RECOVERY_MARGIN`（跑出 [0,1]）、`INVALID_ESCALATE_AFTER`（小于 1）、
  `INVALID_PIPELINE_ID`（标识为空）。
- **不追溯改写**：改之前已产生的告警是历史快照，触发时间、详情、认领信息都不会被改写；
  规则替换为原子整体替换，并发读取不会看到改了一半的规则。

## 兼容范围

三块能力都叠加在原有行为之上：上报的幂等去重、乱序迟到归位、时钟回拨拒绝、
超保留期清理语义不变；没有维护窗口与认领参与时，告警的触发、去重滞回、升级、
自动恢复与之前完全一致；多条流水线各自独立评估，互不影响。

## 配置项（application.properties）

```properties
sla.default-rule.window=PT1H            # 评估窗口
sla.default-rule.max-freshness-lag=PT30M
sla.default-rule.max-schedule-delay=PT10M
sla.default-rule.min-sla-attainment=0.9
sla.default-rule.recovery-margin=0.05   # 恢复滞回裕量
sla.default-rule.escalate-after=3       # 连续违约升级阈值
sla.retention=PT2H                      # 事件保留窗口（实际取 max(retention, 最大评估窗口)）
sla.require-registered-pipelines=false  # 是否拒绝未注册流水线
# sla.pipelines.orders.window=PT30M     # 按流水线覆盖示例
```

## 测试覆盖

- `EventStoreTest`：重复上报幂等、同键冲突、乱序归位、时钟回拨、超保留期、原因可区分。
- `SlaEvaluatorTest`：同组事件不同顺序结论一致、阈值边界（=阈值不违约 / +1s 违约）、
  迟到事件改变结论且可追溯。
- `AlertManagerTest`：阈值抖动不重复告警、连续违约升级、恢复后再违约开启新生命周期。
- `AlertManagerTest`：阈值抖动不重复告警、连续违约升级、恢复后再违约开启新生命周期。
- `MaintenanceServiceTest`：维护窗口声明幂等 / 冲突可区分、按流水线+指标+事件时间的作用域、
  窗口内不拉告警不升级、窗口结束补新告警、窗口内恢复照常、评估结论窗口内照常可查。
- `AlertAckTest`：认领随升级/恢复保留、重复认领幂等不覆盖、已恢复拒绝且原因可区分、
  新生命周期默认未认领。
- `RuleUpdateTest`：部分更新立即生效且不动其他字段、非法入参逐类拒绝且整条不生效、
  历史告警不被追溯改写、同一份事件结论稳定。
- `OpsEndpointsTest`：维护 / 认领 / 在线改规则的端到端 HTTP 链路（含 422/404 可区分原因）。
- `ConcurrencyTest`：8 上报线程 × 2 评估线程并发，断言接受/重复计数精确、
  alertId 唯一、每指标至多一个 FIRING、并发结论与顺序重放一致。
- `ObservabilityControllerTest`：端到端 HTTP 上报/查询/422 拒绝原因。
