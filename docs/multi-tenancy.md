# 多租户：用户额度与用户级公平

本文档说明在既有调度核心（排队、gang 原子启动、取消、节点故障回收、
队列间公平、优先级、预留、抢占）之上新增的多租户能力。

## 1. 用户并发额度

通过 `Config.UserQuotas` 为每个用户配置同时占用资源的上限，
`CPU` / `Mem` / `GPU` 三个维度可独立设置：

```go
s := scheduler.New(scheduler.Config{
    UserQuotas: map[string]scheduler.ResourceVec{
        "alice": {CPU: 32, Mem: 128, GPU: 2},
    },
})
```

规则：

- 同一用户所有**运行中**作业的请求总量（gang 全部任务之和）在任何维度
  都不得超过其额度。
- **未配置额度的用户不受限**，行为与引入额度前完全一致。
- 额度检查以整个 gang 为单位：总额度不够时整组排队，
  不存在“起了一半”或“偷偷超发”。
- 因额度不足被推迟的作业，其 `Defer` 原因为 `UserQuotaExceeded`，
  与既有的 `InsufficientResource` / `ConstraintUnsatisfied` /
  `NoHealthyNode` / `FairShare` 明确区分（额度检查优先于资源检查，
  保证“卡在额度上”不会被误报成资源不足）。

### 额度的释放时机

作业只要不再占用资源，额度**当场**归还，同用户排队作业立即获得机会
（释放路径会自动补一轮调度）：

| 释放路径 | 行为 |
| --- | --- |
| 正常完成 `Complete` | 归还额度，自动补位 |
| 取消 `Cancel` | 归还额度，自动补位 |
| 被抢占重新排队 | 归还额度，可被同用户其他作业使用 |
| 节点故障整组失败 `NodeFailed` | 归还额度，补位到健康节点 |

释放是幂等的：重复或迟到的完成/故障通知不会重复归还额度，
也不会因此超发（额度归还在作业状态从 `Running` 迁出时恰好发生一次，
`QuotaManager.Release` 另有逐维度非负钳制兜底）。

## 2. 队列内用户级公平

通过 `QueueConfig.UserWeights` 配置队列内各用户的权重：

```go
scheduler.QueueConfig{
    Name:        "q1",
    Weight:      1,                            // 队列间权重（既有）
    UserWeights: map[string]float64{"alice": 2, "bob": 1},
}
```

规则：

- 同一队列内多个用户都有排队作业时，调度器按
  **历史已服务量 / 权重** 最小者优先的顺序轮流给机会；
  长期下来服务份额按权重比稳定收敛（如 2:1）。
- 未配置权重的用户权重为 1。
- 单个用户一次性提交的大批量作业不会一直压住其他用户。
- 同一用户内部仍按 **优先级降序、提交时间升序** 服务；
  队首作业不可调度（如约束不满足）不阻塞该用户后续作业。

## 3. 与既有规则的关系

- **队列间公平**：不变。队列之间仍按 `队列主导份额 / 队列权重`
  低者优先排序；用户级公平只决定队列**内部**的机会分配。
- **优先级**：不变。用户内部按优先级排序；抢占仍按优先级判定。
- **预留**：不变。其他队列未用满的保障额度依然不可侵占；
  抢占时仍优先选择超出保障的队列中的低优先级作业。
- **抢占**：被抢占作业回到排队，其用户额度当场释放；
  用户额度检查在资源检查之前，因此被抢占用户的后续作业
  能否补位只取决于额度与资源，与抢占本身无额外耦合。
- **判定兼容**：未配置 `UserQuotas` 与 `UserWeights` 时，
  所有既有场景的判定结果与引入多租户前完全一致
  （回归测试 `TestSubmitAndSchedule` ~ `TestDeferReasons` 覆盖）。

## 4. 兼容范围

- **无额度用户**：不受限，可占满整个集群。
- **默认队列**：提交到未配置队列（或空队列名）的作业进入
  `default` 队列，队列权重 1、无预留、用户权重全为 1。
- **快照恢复**：`Snapshot` / `Restore` 会保存并恢复用户额度占用
  与队列内用户公平计数器，恢复后额度与排队结论与恢复前一致；
  `CheckInvariants` 的资源守恒校验包含用户维度
  （用户额度占用 == 该用户运行中作业请求之和，且不超过其额度上限）。

## 5. 本地验证

```bash
# 全部测试（含上一轮回归用例）
go test ./...

#  verbose 模式可看到每个用例覆盖的业务场景与通过情况
go test ./pkg/scheduler/ -v

# 只跑多租户相关用例
go test ./pkg/scheduler/ -v -run 'TestUserQuota|TestUserWithout|TestQuota|TestUserWeighted|TestUserFairness|TestSnapshot'
```

关键用例对照：

| 业务场景 | 用例 |
| --- | --- |
| 额度刚好用满、再提交即排队（原因可区分） | `TestUserQuotaBoundary` |
| 无额度用户不受限 | `TestUserWithoutQuotaUnlimited` |
| gang 整体受额度约束，不起一半不超发 | `TestUserQuotaGangAtomic` |
| 完成/取消/抢占/节点故障释放额度 | `TestQuotaReleasedOn{Complete,Cancel,Preemption,NodeFailure}` |
| 重复/迟到通知幂等 | `TestQuotaReleaseIdempotent` |
| 多用户加权公平长期份额 2:1 | `TestUserWeightedFairnessLongTerm` |
| 大批量提交不压制其他用户 | `TestUserFairnessNoStarvation` |
| 额度与用户级公平叠加 | `TestUserFairnessWithQuotaInteraction` |
| 快照恢复一致性 | `TestSnapshotRestoreEquivalence` |
