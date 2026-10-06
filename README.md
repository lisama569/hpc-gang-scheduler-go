# hpc-gang-scheduler-go

支持 gang 调度、队列公平、优先级、预留、抢占与多租户（用户额度 + 用户级公平）的 HPC 批调度器。

## 核心能力（既有）

- **gang 原子调度**：一个作业的 `GangSize` 个 worker 要么全部启动、要么整体排队，不存在起一半的状态。
- **排队与优先级**：同队列同用户内按优先级降序、提交先后（FIFO）调度。
- **取消与节点故障**：取消立即释放资源；节点故障时其上 gang 作业整组失败并回收资源。
- **队列间公平**：队列按权重分享集群（虚拟时间加权公平排队，长期份额收敛到权重比）。
- **预留与抢占**：队列可配置 `Guarantee` 预留，未用满预留的队列可回收超用队列的可抢占作业；高优先级作业可抢占低优先级可抢占作业，被抢作业回到队列重新排队。

## 多租户能力（本轮新增）

### 用户并发额度

- `SetUserQuota(user, Resource{CPU, MemGB, GPU})` 为用户配置同时占用上限，三个维度独立判定，任一维度超限即拒绝启动。
- 额度按 **gang 整组总占用**（`Request × GangSize`）记账：不存在“起一半”或偷偷超发。
- **未配置额度的用户完全不受限**，行为与之前版本一致。
- 因额度被拒的作业正常排队，`Explain(jobID)` 返回 `ReasonUserQuotaExceeded`，与 `ReasonInsufficientResource`（资源不足）、`ReasonNodeUnhealthy`（节点不健康）明确区分。

### 队列内用户级公平

- 同一队列内多个用户排队时，按 `SetUserWeight(queue, user, w)` 配置的权重轮流获得调度机会（默认权重 1）。
- 实现为虚拟时间加权公平排队：每调度一个作业，按“作业主导份额 / 权重”推进该用户（及所属队列）的虚拟时间，每轮选择虚拟时间最小者。长期服务份额稳定收敛到权重比（见 `TestUserWeightedFairnessLongTerm`）。
- 没有排队作业的用户/队列，其虚拟时间会对齐到仍有 backlog 一方的最小值，避免长期空闲后回来“霸榜”。

### 与既有机制的关系

- 判定顺序：**队列间公平 → 队列内用户公平 → 用户内优先级/FIFO → 用户额度 → 资源/健康检查 → 抢占**。
- 额度是“准入闸门”，位于公平选择之后、资源放置之前；额度不触顶时，队列公平、优先级、预留、抢占的结论与之前完全一致（`TestQuotaNotBindingKeepsOldBehavior`）。
- 被抢占的作业回到排队队列，其用户额度当场归还；高优先级作业仍可正常触发抢占。

### 额度实时性与一致性

- 正常完成（`Finish`）、取消（`Cancel`）、被抢占重新排队、节点故障整组失败，四条路径都在释放资源的同时归还额度，后续排队作业下一轮即可启动。
- 重复/迟到的完成、取消、故障通知会被忽略，不会重复归还额度，也不会因此超发（`TestNoDoubleRefund`）。
- `Snapshot()`/`Restore()` 完整保存用户额度占用与公平性虚拟时间，恢复后排队结论与后续调度决策与原实例一致；`Validate()` 资源守恒校验包含用户维度（在跑作业总和 == 用户占用 == 节点占用，且不超额度、不超节点容量）。

### 兼容范围

- 未配置额度的用户：不受限，老行为不变。
- 空队列名自动落入 `default` 队列（权重 1）；未配置权重的用户默认权重 1。
- 既有全部用例（gang 原子、排队、取消、节点故障、队列公平、优先级、预留、抢占）在新版本下照旧通过。

## 本地验证

```bash
go build ./...      # 编译
go test ./...       # 全部单测
go test ./scheduler/ -v   #  verbose：每个用例打印覆盖的业务场景与关键结论
```

测试场景一览：

| 用例 | 覆盖场景 |
| --- | --- |
| `TestGangAtomicStart` | gang 原子启动，资源不足整体排队 |
| `TestPriorityThenFIFO` / `TestCancel` / `TestNodeFailure` | 优先级、取消、节点故障回收 |
| `TestQueueWeightedFairness` / `TestPriorityPreemption` / `TestReservationReclaim` | 队列公平、抢占、预留 |
| `TestUserQuotaBoundary` / `TestUserQuotaGangAtomic` / `TestUserQuotaMultiDimension` | 用户额度边界、gang 整体计额、多维额度 |
| `TestNoQuotaUserUnlimited` / `TestQuotaNotBindingKeepsOldBehavior` | 无额度用户与兼容性 |
| `TestQuotaReleasePaths` / `TestNoDoubleRefund` | 完成/取消/抢占/故障四条释放路径、防重复归还 |
| `TestUserWeightedShareSingleShot` / `TestUserWeightedFairnessLongTerm` / `TestQueueAndUserFairnessCompose` | 用户级加权公平与长期收敛 |
| `TestSnapshotRestoreEquivalence` / `TestSnapshotThenNodeFailure` | 快照恢复一致性 |
