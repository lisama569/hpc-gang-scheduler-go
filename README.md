# HPC Gang 调度核心（Go，内存可验证实现）

一个无外部依赖、可本地独立验证的 HPC 作业调度核心。以内存中的节点与作业模型模拟
**排队 → gang 原子启动 → 运行 → 完成/取消/故障回收** 的完整生命周期，并支持
加权公平份额、严格优先级、受控抢占、预留防饿死、点位/拓扑约束与快照恢复。

所有公开操作在同一把互斥锁下串行化，因此任意并发操作序列下：

* 不会部分启动 gang、不会超量分配；
* 重复/乱序事件幂等，不重复扣款、不状态回退；
* 调度结论由确定性排序唯一决定，可复现、可审计。

## 目录结构

```
scheduler/
  types.go        资源、节点、作业、状态枚举与可区分拒绝原因
  errors.go       领域错误（ErrNotFound / ErrAlreadyExists / ErrDuplicateEvent ...）
  scheduler.go    调度核心：提交、取消、完成、调度主循环、资源提交/释放
  gang.go         gang 原子放置：资源维度检查、健康过滤、SameRack/白名单拓扑
  fairshare.go    队列配置 + 加权轮转/优先级/预留排序
  preemption.go   受控抢占（影子视图验证、审计）
  events.go       节点故障/下线/恢复事件（eventID 幂等去重）
  ledger.go       快照/恢复与资源守恒校验
  audit.go        审计记录类型
cmd/demo/         可运行演示，打印关键输入、判定依据与资源变化
```

## 快速验证

```bash
go build ./...
go vet ./...

# 完整测试（含竞态检测；其中并发风暴用例较重，约 1-2 分钟）
go test -race -count=1 ./...

# 跳过重型并发用例的快速回归
go test -race -count=1 -short ./...

# 可观察的端到端演示（日志含调度判定与资源变化）
go run ./cmd/demo
```

## 作业状态机

```
            Submit(开放队列)                    gang 全部成员放置成功
   (无) ───────────────────▶ Pending ───────────────────────────────▶ Running
                                │                                      │
             关闭队列提交        │  Cancel(running)                     ├─ NotifyComplete ─▶ Completed
                                 ▼                                     ├─ Cancel ──────────▶ Cancelled
                             Rejected                                  └─ 成员节点故障 ────▶ Failed
       Pending ──Cancel──▶ Cancelled
```

* `Pending`：排队等待。资源/拓扑不满足时**保持 Pending**，作业上记录最近一次可区分的
  `RejectReason` 与人类可读细节，并累加 `Retries`；下一轮会被再次评估。
* `Running`：gang 的 `Members` 个成员已**一次性**原子提交到具体节点（`NodeIDs` 长度恒等于
  `Members`）。
* 终态：`Completed` / `Cancelled` / `Rejected` / `Failed`。终态不可回退；对终态作业的
  迟到完成/取消通知返回 `ErrTerminalState`，不产生任何资源副作用。

### 可区分的拒绝原因（`RejectReason`）

| 原因 | 含义 |
| --- | --- |
| `InsufficientCPU` / `InsufficientMemory` / `InsufficientGPU` | 单机或全局对应维度资源不足（先判单机单成员，再判总量） |
| `NoHealthyNode` | 候选集合中没有健康节点（全部故障/下线） |
| `ConstraintUnsatisfiable` | 白名单引用未知节点、`SameRack` 下无单一机架可容纳全体成员、或资源碎片化导致按节点切分装不下 |
| `QueueClosed` | 提交到已关闭队列，作业直接进入 `Rejected` |
| `GangIncomplete` | 内部/编程错误：引用了未注册的 gang |

放置判定是纯函数式的：`tryPlaceGang` **只计算方案、不改状态**；只有当全部成员都有合法
落点时才由 `commitPlacement` 一次性占用资源，因此“部分启动”在结构上不可能发生。

## 资源与拓扑约束

* 资源三维：CPU、Memory、GPU，均为整数；每成员需求相同（同构 gang）。
  节点可容纳的成员数 = 各维度 `可用量 / 需求量` 的最小值；零需求维度不参与约束。
* `PlacementConstraint`：
  * `Nodes`：白名单，成员只能落在这些节点；含未知节点名 ⇒ `ConstraintUnsatisfiable`。
  * `SameRack`：全部成员必须在同一 `Rack`；按机架名升序选**第一个**可行机架。
* 只有 `Healthy` 节点承接新成员；`Drained`（cordon）节点不接新作业但允许已有作业跑完；
  `Faulted` 节点不参与调度。
* 放置按节点 ID 升序贪心填充；相同输入下结果逐位一致。

## 公平、优先级与预留

调度顺序（每成功放置一个 gang 后立即重算，使份额在**同一轮内**即时生效）：

1. **预留地位**（防饿死）：被抢占过的作业，或被推迟次数达到队列
   `ReservationThreshold` 的作业，排在最前。
2. **加权轮转**：比较 `队列累计服务槽位 / 权重`，比值小者优先（权重 1:1 时严格轮转；
   权重 2:1 时长期份额按 2:1 收敛）。
3. 队列内**严格优先级**高者优先。
4. 同优先级按提交时间 FIFO。
5. 最后以 `GangID` 字典序兜底，保证 map 迭代等不确定因素下结论仍完全可复现。

一轮调度中因资源不足被推迟的 gang 本轮不再重试，避免与后续 gang 互相饿死；下一轮重新参与。

## 抢占规则（可控、可审计）

仅在以下全部条件成立时才允许抢占，且有影子视图验证，保证不会误杀：

* 目标 gang 所在队列显式 `PreemptEnabled`；
* 目标 gang 为高优先级（`Priority>0`）或已获得预留地位；
* 受害者与目标**同队列**、正在运行、优先级**严格更低**（跨队列永不抢占）；
* 按 `(优先级升序, 已运行时长最长, 作业ID)` 确定性挑选受害者；
* 每挑一个受害者就在“影子资源视图”上重新验证，**任何受害者序列都无法腾出完整放置时，
  一个都不抢**，状态零修改。

抢占发生后：受害者整 gang 释放资源、回到 `Pending` 并记 `PreemptCount`（获得预留地位，
下一轮排在队首），每个受害者写入一条 `preempt` 审计记录（`AuditLog()` 可查）。
抢占与预留的兑现也会写 `reserve` 记录。被影响作业的下一次调度结论由确定性排序与确定性
放置保证可复现。

## 故障恢复语义与幂等事件

* `NodeFault(nodeID, eventID)`：节点 → `Faulted`；任何在该节点上有成员的运行中 gang
  **整体 Failed**（gang 不允许部分存活），其在其他健康节点上的份额一并回收。
* `NodeDrain(nodeID, eventID)`：节点 → `Drained`（cordon），已有作业继续运行。
* `NodeRecover(nodeID, eventID)`：节点 → `Healthy`，重新参与调度。
* 所有事件以 `eventID` 去重：相同 `eventID` 重复到达返回 `ErrDuplicateEvent` 且不生效；
  不同 `eventID` 但节点已是目标状态时为幂等空操作。完成通知 `NotifyComplete` 同样按
  `eventID` 去重；迟到事件返回 `ErrTerminalState`，绝不重复释放或回退状态。

## 快照与资源守恒

* `Snapshot()` 导出节点、作业、队列配置、已消费事件 ID、审计记录的深拷贝。
* `Restore(snap)` 得到等价调度器（含事件去重状态），可继续调度，结论与原实例一致。
* `CheckConservation()` 在任意操作序列后校验：
  * 每台节点 `Used` 恰好等于运行中作业在该节点占位之和；
  * 运行中作业的 `NodeIDs` 长度等于 `Members`，且不含故障节点；
  * 无负用量、无超量分配；非运行中作业不持有任何占位。

## 并发模型

调度器为“单锁 + 内存状态”的确定性内核：提交/取消/完成/节点事件/调度/快照全部互斥。
测试中以多 goroutine 并发制造提交、取消、完成、节点故障/恢复交错（含同一 eventID 风暴），
在 `-race` 下验证无数据竞争且账本始终守恒。日志（注入 `Logger` 即可）会打印：
作业声明输入、每次放置的拒绝原因与细节、逐节点资源增减、抢占受害者与节点状态迁移。
