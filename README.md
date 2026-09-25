# Pipeline SLA Observability Service

流水线可观测性服务（Java 21 + Spring Boot 3.4，进程内存储）：

- 接收作业运行事件上报（开始 / 成功 / 失败），具备**幂等、乱序/迟到/时钟回拨的确定语义**，
  无法归属或与既有记录矛盾的上报一律拒绝并返回**可区分的原因**，绝不静默丢弃或部分生效；
- 按**可配置窗口与阈值**评估每条流水线的新鲜度、调度延迟与 SLA 达标状态，
  同一组事件 + 同一评估时刻的结论**可复现**，历史结论因迟到事件而变化时**可追溯到具体事件**，
  存储与查询开销受保留范围与环形历史约束，不随历史无限劣化；
- 维护告警完整生命周期：**去重触发、持续违约升级、迟滞自动恢复**，
  阈值附近抖动不产生重复告警，恢复后再次违约开启全新生命周期，
  并发上报与并发评估下不丢失、不重复、不倒退。

## 1. 构建与本地启动

```bash
# 需要 JDK 21+（本工程在 JDK 25 上构建通过）与 Maven 3.9+
mvn package
java -jar target/pipeline-sla-observability-java-1.0-SNAPSHOT.jar
# 默认端口 8080；覆盖：--server.port=18080
```

启动时会从 `src/main/resources/application.properties` 注册两条示例流水线
（`orders-hourly`、`payments-5min`）。也可通过管理接口运行时动态注册。

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events` | 上报单个事件 |
| POST | `/api/v1/events/batch` | 批量上报（逐条独立给出结论） |
| GET | `/api/v1/pipelines` | 流水线清单与配置 |
| GET | `/api/v1/pipelines/{id}/latest` | 最新一次评估结论 |
| GET | `/api/v1/pipelines/{id}/history?limit=n` | 评估历史（最新在前，有界） |
| POST | `/api/v1/pipelines/{id}/evaluate?asOf=...` | 立即评估；`asOf` 缺省为当前时刻，给定则为**只读历史重放** |
| GET | `/api/v1/pipelines/{id}/alerts/active` | 未恢复告警 |
| GET | `/api/v1/pipelines/{id}/alerts/recovered` | 已恢复告警历史 |
| POST | `/api/v1/pipelines/{id}/alerts/{alertId}/recover` | 手动恢复告警 |
| GET | `/api/v1/pipelines/alerts/active` | 全部流水线的未恢复告警 |
| POST | `/api/v1/admin/pipelines` | 运行时注册流水线 |

事件上报体（时间均为 UTC ISO-8601，例如 `2026-09-26T00:00:00Z`）：

```json
{
  "pipelineId": "orders-hourly",
  "runId": "2026-09-26T00-run",
  "eventId": "evt-0001",
  "eventType": "STARTED",
  "eventTime": "2026-09-26T00:00:00Z",
  "scheduledTime": "2026-09-25T23:59:30Z"
}
```

处理结论（HTTP 恒为 200，语义在响应体内；流水线不存在返回 404）：

```json
{ "outcome": "ACCEPTED | DUPLICATE | REJECTED",
  "eventId": "evt-0001",
  "reason": null,
  "message": "identical event already accepted as ingestSeq=1; no state change",
  "ingestSeq": 1 }
```

## 3. 事件语义（ingestion）

### 3.1 关键时间与序号

| 概念 | 含义 |
| --- | --- |
| `eventTime` | 事件实际发生时间，来自上报方时钟；指标计算与排序的依据 |
| `scheduledTime` | 计划调度时间，仅 `STARTED` 使用，用于调度延迟 |
| `receivedAt` | 服务端接收墙上时间，仅用于时间合理性校验与留痕，**不参与排序** |
| `ingestSeq` | 服务端全局单调递增接收序号，**与墙上时钟无关**，同刻事件用它决胜 |

### 3.2 处理规则（按顺序执行，先校验后写入，拒绝不会产生部分生效）

1. **MALFORMED**：`pipelineId/runId/eventId/eventType/eventTime` 缺失、`eventType` 取值非法、
   `STARTED` 的 `scheduledTime` 晚于 `eventTime`。
2. **FUTURE_EVENT_TIME**：`eventTime > receivedAt + futureToleranceSeconds`。
   上报方时钟快出容差通常意味着误报，拒绝而非“等待未来”。
3. **EXPIRED**：`eventTime < receivedAt - retentionSeconds`。
   事件已超出可追溯窗口，接受它会永久改变无法再解释的远古结论，故显式拒绝。
4. **幂等**：以 `eventId` 为全局幂等键。载荷完全一致的重复上报返回 `DUPLICATE`
   与**首次接收时的序号**，不重复计数、不重跑评估、不改变任何结论。
5. **CONFLICT（矛盾拒绝）**：
   - 同一 `eventId` 已存在但载荷不一致；
   - 同一 `runId` 出现第二个 `STARTED`（重试必须使用新 runId，重发必须复用 eventId）；
   - 同一 run 出现第二个终态，或 `SUCCEEDED` 与 `FAILED` 并存；
   - 终态 `eventTime` 早于已知的 `STARTED.eventTime`（时间倒挂）。
   - 注意：**终态先到、STARTED 后到**属于合法乱序，照常接受。

### 3.3 乱序、迟到与时钟回拨的确定语义

- 事件落库后进入按 `(eventTime, ingestSeq)` 排序的有序结构。事件以什么顺序到达不影响全序，
  因此评估结论是 `(pipelineId, asOf, asOf 之前已落库事件集合)` 的**纯函数**。
- **迟到事件**：只要落在 `retentionSeconds` 保留范围内就正常落库，参与下一次评估；
  它可能让某个历史时刻的指标从 BREACH 变为 PASS（例如迟到的成功事件证明当时其实刚跑完），
  该变化通过相邻两次评估记录及其 `basedOnEventSeqs` 可追溯。需要重放历史时刻可用
  `evaluate?asOf=...`（只读，不推进评估序号、不影响告警）。
- **时钟回拨**：`ingestSeq` 单调递增保证先后关系不被墙上时钟破坏；`receivedAt` 回拨也不影响
  事件排序。回拨恢复后到达的迟到事件按正常规则处理。

## 4. 评估规则（evaluation）

每条流水线配置（可来自配置文件或管理接口）：

| 配置 | 含义 |
| --- | --- |
| `windowSeconds` | 评估窗口长度，只统计 `[asOf-window, asOf]` 内的事件（按 eventTime） |
| `retentionSeconds` | 迟到事件可追溯范围；每次评估顺带裁剪更早的事件与空 run（开销有界） |
| `futureToleranceSeconds` | eventTime 领先服务端时钟的最大容差 |
| `thresholds.*` | 各指标阈值、升级与恢复延迟 |

三个指标：

1. **新鲜度 FRESHNESS**（秒）：`asOf - 窗口内最近一次终态事件时间`。
   `age > maxFreshnessSeconds` 违约；窗口内无终态 → **不观测**，不误报。
2. **调度延迟 SCHEDULE_DELAY**（秒）：窗口内带 `scheduledTime` 的 STARTED 中
   `eventTime - scheduledTime` 的**最大值**；`delay > maxScheduleDelaySeconds` 违约；
   无此类 STARTED → 不观测。
3. **失败率 FAILURE_RATE**（%）：窗口内失败终态 / 全部终态 × 100；
   严格 `rate > maxFailureRatePercent` 才违约（阈值为 0 时，任何失败即违约）；无终态 → 不观测。

汇总 SLA：任一**已观测**指标违约 → `BREACH`；全部已观测且达标 → `PASS`；没有任何观测 → `UNKNOWN`。

每次正式评估都持久化一条记录：`seq`（流水线内单调递增）、`asOf/windowStart`、三个指标的
取值/阈值/布尔违约/**人读判定依据 detail**、以及 `basedOnEventSeqs`（参与计算的事件接收序号）。
评估历史按 `evaluation-history-limit` 环形淘汰；事件按 retention 裁剪。
因此无论持续上报多久，单次评估扫描的事件数、历史查询返回的记录数都是有界的。

## 5. 告警生命周期（alert）

去重维度为 `(pipelineId, metricType)`，同一维度同一时刻至多一个未恢复告警。
状态机只允许前进：`FIRING → ESCALATED → RECOVERED`（或 `FIRING → RECOVERED`），
每次迁移都写入 `transitions`（时间、前后状态、原因、指标值、评估序号）。

- **触发**：无未恢复告警且指标违约 → 新建 `FIRING`。已有未恢复告警时再违约，
  只刷新 `lastBreachAt` 与最新违约值，**绝不重复触发**。
- **抖动抑制**：指标在阈值附近来回切换只刷新 `lastBreachAt/lastNormalAt`。
  只有当 `asOf - lastBreachAt ≥ recoveryDelaySeconds`（恢复观察期持续满足）才自动恢复；
  观察期内一次抖动即刷新违约时刻，恢复计时作废。
- **升级**：`FIRING` 告警自触发起持续未恢复达到 `escalationDelaySeconds` 且本次评估仍违约
  → `ESCALATED`，只升级一次。
- **自动恢复**：恢复时记录恢复指标值与原因 `AUTO_RECOVERED`，移入有界的已恢复历史
  （`recovered-alert-limit`），从活跃集合移除。
- **新生命周期**：恢复后再次违约会生成**全新 id** 的告警与完整迁移链，与旧告警互不影响。
- **无观测**：指标 UNKNOWN 时既不触发也不恢复，避免数据缺口造成误报/误恢复。
- 也可调用恢复接口执行 `MANUAL_RECOVERED`；已恢复告警不会被任何后续操作改写（状态不倒退）。

并发模型：每条流水线一把锁。事件“落库 → 评估 → 告警对账”在同一把锁内原子完成，
后台定时评估（`pipeline.observability.evaluation-interval-seconds`，默认 30s）也取同一把锁；
不同流水线之间完全并行。

## 6. 本地快速验证

```bash
mvn test                      # 29 个单测/集成测试
mvn spring-boot:run           # 或 java -jar 启动
```

冒烟 curl（启动后）：

```bash
BASE=http://localhost:8080/api/v1
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# 上报开始
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d "{\"pipelineId\":\"orders-hourly\",\"runId\":\"r1\",\"eventId\":\"e1\",
       \"eventType\":\"STARTED\",\"eventTime\":\"$NOW\",\"scheduledTime\":\"$NOW\"}"
# 完全相同的再发一次 -> DUPLICATE（ingestSeq 不变）
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d "{\"pipelineId\":\"orders-hourly\",\"runId\":\"r1\",\"eventId\":\"e1\",
       \"eventType\":\"STARTED\",\"eventTime\":\"$NOW\",\"scheduledTime\":\"$NOW\"}"
# 失败终态 -> FAILURE_RATE=100% 违约 -> 自动出现 FIRING 告警
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d "{\"pipelineId\":\"orders-hourly\",\"runId\":\"r1\",\"eventId\":\"e2\",
       \"eventType\":\"FAILED\",\"eventTime\":\"$NOW\"}"
curl -s $BASE/pipelines/orders-hourly/alerts/active
curl -s $BASE/pipelines/orders-hourly/latest
```

## 7. 测试覆盖

- `IngestServiceTest`（9）：正常生命周期、幂等重复、载荷矛盾、重复终态/异种终态/时间倒挂、
  迟到边界（闭区间）、未来时间边界、时钟回拨序号单调性、乱序到达结论一致、未注册/缺字段/非法类型。
- `EvaluationServiceTest`（8）：新鲜度/调度延迟/失败率边界、窗口排除、UNKNOWN、
  同 asOf 可复现、迟到事件改变历史结论且 `basedOnEventSeqs` 可追溯、历史环形淘汰、过期裁剪。
- `AlertLifecycleTest`（6）：阈值抖动单告警、升级恰好一次、恢复观察期迟滞、
  恢复后再违约开新生命周期、已恢复不倒退、无观测不触发。
- `ConcurrencyTest`（2）：8 上报线程 ×100 运行 + 2 评估线程并发，
  断言事件不丢不重、接收序号连续无空洞、每 run 终态唯一、告警迁移严格前进；
  以及同一 eventId 并发 16 次上报恰好 1 次 ACCEPTED、15 次 DUPLICATE。
- `WebApiIntegrationTest`（3）：端到端上报/幂等/拒绝/查询、动态注册与 404、手动评估与历史重放。

测试用 `MutableClock` 显式推进/回拨时间以保证结论可复现；关键用例通过日志打印
**输入事件（INPUT）与判定依据（DECIDE/指标 detail）**，可在
`target/surefire-reports/` 中查看。

## 8. 进程内存储说明

数据全部保存在内存中，重启即清空，适合本地验证与单进程部署。
关键不变量（不丢不重、序号连续、状态机前进、有界淘汰）均由单流水线锁与全局原子序号保证；
如需水平扩展，可在保持上述语义的前提下把 `PipelineRegistry` 换成带行锁/事务的持久化实现。
