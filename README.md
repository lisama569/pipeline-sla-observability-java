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

# 运行测试（单测日志会打印输入事件与判定依据）
mvn test
```

## 告警日常处置（维护静默 / 认领 / 在线改规则）

三块能力都叠加在原有行为之上：没有维护窗口和认领参与时，告警的触发、去重、升级、
恢复语义与之前完全一致；上报的幂等、乱序、时钟回拨、超期清理语义不变；
多条流水线各自独立评估，互不影响。

### 计划内维护 / 静默

```bash
# 声明维护窗口（metricType 省略 = 静默该流水线全部指标；窗口按事件时间 [start, end) 判断）
curl -X POST localhost:8080/api/maintenance -H 'Content-Type: application/json' -d '{
  "pipelineId":"orders","start":"2026-10-07T01:00:00Z","end":"2026-10-07T02:00:00Z",
  "note":"计划换版"}'
# 只静默单个指标：加上 "metricType":"SLA_ATTAINMENT"

# 重复提交同一窗口 -> status=DUPLICATE，不会产生第二份记录（幂等键 = pipelineId|metric|start|end）
# 查询 / 撤销
curl 'localhost:8080/api/maintenance?pipelineId=orders'
curl -X POST localhost:8080/api/maintenance/revoke -H 'Content-Type: application/json' -d '{
  "pipelineId":"orders","start":"2026-10-07T01:00:00Z","end":"2026-10-07T02:00:00Z"}'
```

- 窗口内的违约：评估结论照常可从 `/api/pipelines/{id}/assessment` 查到，
  但不新拉告警、不把已有告警往上升级、不计连续违约次数；带裕量恢复照常生效。
- 窗口一过（按事件时间水位线判断，不用机器当前时间，重放结果一致），
  指标仍在违约则照常形成新告警。
- 声明与撤销可并发，不会互相覆盖或丢数据；非法入参返回 422，
  原因可区分：`MISSING_FIELDS` / `INVALID_TIME_RANGE`。

### 告警认领

```bash
# 先查出活动告警的 alertId
curl 'localhost:8080/api/alerts'
# 认领（带认领人与备注）；alertId 含 | 和 :，放进路径前需 URL 编码
# （例：orders|SLA_ATTAINMENT|... -> orders%7CSLA_ATTAINMENT%7C...）
curl -X POST localhost:8080/api/alerts/<alertId>/ack -H 'Content-Type: application/json' \
  -d '{"ackBy":"lisa","note":"值班处理中"}'
```

- 认领后该告警继续违约不会新开告警，后续评估也不会把状态倒回去；
  认领信息（`acknowledgedBy/acknowledgedAt/ackNote`）随告警快照保留到恢复（含升级过程）。
- 恢复后再次违约 = 新的事故周期：生成全新 `alertId`，默认未认领。
- 重复认领幂等：返回 `DUPLICATE` 与既有快照，不覆盖、不报错。
- 已恢复的告警再认领返回 422 + `ALERT_ALREADY_RESOLVED`；
  告警不存在返回 422 + `ALERT_NOT_FOUND`；缺认领人返回 422 + `MISSING_ACK_BY`，三种原因可区分。

### 在线改规则

```bash
# 只改 orders 这条流水线的部分阈值（未携带的字段保持原值），立即生效、无需重启
curl -X PUT localhost:8080/api/rules/orders -H 'Content-Type: application/json' -d '{
  "window":"PT30M","maxFreshnessLag":"PT5M","minSlaAttainment":0.5}'
# 查看当前生效规则
curl localhost:8080/api/rules/orders
```

- 生效范围：后续评估与上报；已产生的告警快照不被追溯改写。
- 原子生效：先整体校验，任一字段非法则整条拒绝（不会只改一半），
  原因可区分：`INVALID_WINDOW`（窗口非正数）、`INVALID_FRESHNESS_LAG` /
  `INVALID_SCHEDULE_DELAY`（阈值为负）、`INVALID_ATTAINMENT` / `INVALID_RECOVERY_MARGIN`
  （跑出 [0,1]）、`INVALID_ESCALATE_AFTER`（小于 1）、`NO_FIELDS`、`MISSING_PIPELINE_ID`。
- 同一规则下同一份事件的评估结论稳定一致；规则对象整体替换，
  并发读取不会看到改了一半的规则。

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
- `MaintenanceServiceTest`：维护窗口声明幂等、按事件时间 [start,end) 静默、按指标静默、
  撤销可区分、并发声明/撤销不产生重复记录。
- `MaintenanceAlertTest`：窗口内不新拉不升级、窗口结束仍违约必触发新告警、
  恢复不受静默影响、指标级静默不影响其它指标、撤销后立即恢复原有行为。
- `AlertAckTest`：认领信息随快照保留到恢复、重复认领幂等、已恢复/不存在/缺认领人原因可区分、
  新事故周期默认未认领。
- `RuleUpdateServiceTest`：部分更新立即生效且不动其它字段、各类非法入参原因可区分、
  校验失败整条不生效、已产生告警不被追溯改写、同规则同事件结论稳定。
- `OperationsControllerTest`：三块能力的端到端 HTTP 链路（声明/撤销/认领/改规则/422 原因）。
- `OperationsConcurrencyTest`：并发上报 + 评估 + 维护声明 + 认领，
  断言不丢告警、不重复触发、状态不倒退、认领不丢不重。
- `ConcurrencyTest`：8 上报线程 × 2 评估线程并发，断言接受/重复计数精确、
  alertId 唯一、每指标至多一个 FIRING、并发结论与顺序重放一致。
- `ObservabilityControllerTest`：端到端 HTTP 上报/查询/422 拒绝原因。
