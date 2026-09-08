# PR 与 `dev/v0.2.0` 的任务组和批处理对比

## 范围与结论

比较基线为 `feat/parallel-task-group-builder` 相对 `dev/v0.2.0` 的差异。PR 新增的是一个
**固定、异构、具名任务集**的 API；既有 batch 是一个**同构、按输入顺序、可滑动窗口提交**
的 map API。两者不能互相替代。

终态词汇已通过 `TaskOutcome` 统一（旧 `internal.FutureState` 已删除），任务准备/提交
的低层步骤已抽取到 `internal/TaskSubmissions` 并被两侧共用。2026-09-04 已落地两项优化：
成员 deadline 不严于组 deadline 时跳过冗余的成员 bind（group 从 N+1 个 timer 回到
"一个任务集合一个超时任务"），以及命名/javadoc 清理（删除 `TaskOutcome.isTerminal()`、
`TaskGroupMemberResult.completionReason()` 改名 `outcome()`）。剩余优化为机械去重与
提交侧收口；聚合结果和取消编排应保留各自模型。

## 新增类

| 新增类 | 对应的 batch 组件 | 职责与差异 |
| --- | --- | --- |
| `TaskGroup` / `TaskGroupSpec` | `Par.map` + `ConcurrentLimitExecutor` | 都发起多个任务并绑定取消。前者在所有成员 future 已创建、handle 已绑定后才统一提交，成员可异构、具名、使用不同 `Par`；后者按 list 同构映射，可异步滑动窗口补充提交。`TaskGroupSpec` 是可复用的组定义，替代了早期的 builder。 |
| `TaskGroupResult` | `AsyncBatchResult` | 都承载多任务结果。group 是一次终态快照，按名称保存成员明细、组级原因和时间；batch 立刻返回 ordered futures，`report()` 是可重复读取的当前状态统计。 |
| `TaskGroupMemberResult` | `ListenableFuture<T>` + `FutureInspector` | 都表达单任务终态。group 固化 `TaskContext`、failure 与 `TaskOutcome`（`outcome()`）；batch 仅暴露 future，需要在调用时检查。 |
| `TaskGroupCompletionReason` | `CancellationToken.State`（间接） | group 对外收敛为 `SUCCESS/FAILED/TIMEOUT/CANCELED`；batch 没有批次完成原因。 |
| `TaskOutcome` | 删除的 `internal.FutureState` | 实质统一：区分用户失败、提交失败、直接取消、组取消、fail-fast、超时的共同词汇。batch 的 `AsyncBatchResult.report()` 已使用它。 |
| `TaskGroupListener` | `TaskListener` | 都是观测扩展点，但粒度不同：后者每个 callable 执行完回调，前者在全组收敛后回调一次。保留两个 SPI。 |
| `SubmissionException` | batch 的提交失败 future | 给"用户代码尚未执行就提交失败"一个可识别类型，供 group 写入 `SUBMISSION_FAILURE`，同时被 `FutureInspector` 识别。 |
| `TaskRef` | — | 具名成员的句柄，group 特有。 |

`MultiTaskOptions`（组与批次共用的选项类，字段取并集：批次读 name/parallelism/timeout/
taskType/rejectEnqueue，组读 name/timeout/listeners，成员按批次子集读取并忽略
`parallelism`）与 `MultiTaskContext`（解析后的共享上下文，同时服务 `Par.map` 与 group
成员）由既有类合并/重命名而来，迁移说明见 `docs/en/migration-v0.2.md`。

## 共享能力与不可合并边界

### 已统一或可共享

1. **终态词汇已统一。** `TaskOutcome` 同时由 group 成员快照和 batch 报告使用；
   `FutureInspector` 对 `ExecutionPhaseHintFuture` 保留提交失败信息。
2. **任务准备已共用。** batch（`Par.executeGlobal()`）与 group（`Par.prepareGroupTask()`
   + `TaskSubmissions.submitScoped`）都经 `internal/TaskSubmissions.prepare()` 创建
   `TaskExecutionContext`、`ScopedCallable`、TTL 包装和 phase future。
3. **共同基础设施相同。** 两条路径都复用 `MultiTaskContext`、`CancellationToken`、
   task graph、phase observer、task listener 与 `GlobalPar.retainUntilComplete()`。
4. **超时归因链已打通。** `CancellationToken.originState()` 沿 parent 链找首个非传播
   终态，使"外层 scope 超时导致本组被取消"归因 `TIMEOUT` 而非 `CANCELED`；其余根因
   （`MUTUAL_CANCELED`/`FAIL_FAST_CANCELED`）仍归为取消。
5. **成员 bind 已按需跳过。** `TaskGroup.start()` 仅当成员 deadline 严于组 deadline 时才
   执行成员 bind 和超时升级 listener；继承组超时的成员不产生成员级 timer/包装 future。
   跳过路径的安全性依据：向下传播走构造器 listener（不依赖 `bind()`），成员 future 的
   取消由 group bind 直接覆盖；未 bind 的成员 token 永停 `RUNNING`（隐式约束，代码有
   注释），`classifyCancelled` 回退 group token 状态得同样的 `TIMEOUT` 归因。等价性由
   `TaskGroupTest` 的成员独立超时 / 组超时 / 混合 deadline / 嵌套传播用例锁定。

### 必须保留的差异

1. **提交边界不同。** group 必须先完成全部定义、future 和 `TaskRef` 的绑定，再设置成员
   监听和取消绑定，最后提交；这保证成员失败不会让尚未暴露的成员逃逸。batch 的滑动窗口
   故意允许一部分 placeholder 尚未真正提交。
2. **取消拓扑不同。** batch 只绑定一个 batch token 到 futures 和 submit canceller。group
   有 group token 及成员 token：成员独立 deadline 必须先升级为 group timeout，才能避免
   被 group 的 fail-fast 误归因为失败。
3. **结果形态不同。** batch 的顺序和部分可提交状态是调用者的控制面；group 的名字、异构
   类型和最终收敛原因是结构化并发的控制面。

## 剩余优化点与 ROI 排序（2026-09-04，T1/T2 已落地后重排）

按 ROI（收益/成本）从高到低。

### T1：机械去重（中收益，低成本，风险极低）

按抽取顺序：

1. **观测上下文解析**：`Par.java` 与 `TaskGroup.java` 各有一段相同的 8 行三元链，下沉为
   `MultiTaskContext` 或 internal 静态 helper。
2. **deadline 溢出安全计算**：共 3 份（`MultiTaskContext.resolve` 内部两次 +
   `TaskGroup.deadline()`），且精度不一致——`MultiTaskContext` 走 `toMillis()` 再乘回
   纳秒（丢亚毫秒精度），`TaskGroup` 直接用 `toNanos()`。抽共享 `deadlineFrom(now,
   timeout)` 并统一纳秒精度。
3. **终态识别**：`TaskGroup.memberCompleted` 手写的 `isCancelled()/get()/
   ExecutionException/SubmissionException` 判定与 `ExecutionPhaseHintFuture.outcome()`、
   `FutureInspector.state/exceptionNow` 是第三份拷贝。非 cancelled 分支改用
   `FutureInspector`；cancelled 分支的 token 归因必须保留（plain future 不知道取消来源，
   且成员 timeout > group token 状态 > 直接取消的优先级不能丢）。
4. **switch 归因**：`classifyCancelled` 与 `deriveCompletionReason` 对
   `groupToken.state()` + `originState()` 的 TIMEOUT 判定重复，抽 `isTimeoutOrigin()`
   私有方法。
5. **TaskGroup 内部小合并**：`buildWhileOpen` 双循环合一（删 `memberPars` 列表）；
   `completeEmpty` 与 `convergeIfTerminal` 尾部重复的 set+notify 合并。

### T2：提交侧收口（中收益，中高成本，可延后）

`TaskSubmissions.submitScoped` 与 `ConcurrentLimitExecutor.fallbackSubmit` 各装一次
`SubmissionScope`，CPU inline 回退逻辑分裂在 `ListenableCompletionService.
submitOrRunInline` 与 `ExecutionPhaseHintFuture.submitPrepared` 两处。batch 需要
completion-queue 注册，不能完全合并；可把 SubmissionScope 安装与"CPU 可 inline"判定
收口到一处。

注意其中隐含语义漂移：同一个 rejected 任务，CPU 任务两侧都 inline 跑，但 IO 任务被拒时
group 记 `SUBMISSION_FAILURE` 而 batch 整批失败。即使不做代码收口，也应先把该差异写进
文档。

### T3：API 收窄（低收益，成本取决于测试搬迁，暂不做）

- `MultiTaskContext.resolve` 4 参 public 重载自述 "for compatibility and tests"：删除需
  把跨包测试改到 6 参重载，0.x 可做但优先级最低。
- `CancellationToken.addStateListener()` 收窄需改 `bind` 签名（如加 escalation target
  参数）：成员 bind 按需跳过后，该 listener 只在成员有更严 deadline 时注册，用量已大幅
  下降；不动 bind 签名则维持现状。不能泛化为"子超时传染父"：嵌套 batch 的子超时不应
  升级父 token 为 `TIMEOUT_CANCELED`。
- `GlobalPar.timeoutScheduler()` 每次调用新建包装：成员 bind 跳过后常见场景每组只调 1
  次，不处理。

### 建议实施顺序

1. T1——纯重构，一次提交。
2. T2 先把 rejection 语义差异写进文档，代码收口视后续变更频率决定。

## 不建议的改动

- 不合并 group/batch 公开结果模型（`TaskGroupResult` vs `AsyncBatchResult`）：frozen
  build + 按名归因 vs 滑动窗口 + 按序 placeholder，拓扑不同。
- 不把成员 bind 的 timer 合并进 group bind：成员独立 deadline 必须先触发并升级为
  `TIMEOUT`，否则成员超时被误归为 fail-fast。成员 bind 按需跳过后此优化已无价值。
- 不做 timer 调度去重（共享 deadline registry / 时间轮）：复杂度高，常见场景每组只剩
  一个定时任务。
- 不给 batch 补终态聚合 API：会改变 `AsyncBatchResult` 的结果语义且需要决定
  sliding-window 未提交 placeholder 的终态定义，属独立产品决策。
- 不合并 `TaskGroupListener` 与 `TaskListener`：粒度与触发时机不同。
- 不让多个 batch/group 共用同一个 token 实例：token 状态是单值终态机，一个 scope 的
  `FAIL_FAST_CANCELED` 会污染另一个 scope 的归因。

## CancellationToken 背景：多层嵌套传播

向下传播是完备的，但原因被压平：构造器 listener 对任意深度成立（父 token 任意负值终态
→ 子 `PROPAGATING_CANCELED` → 子的 futureToken 取消 → 孙 token 继续级联），但每一跳
原因都被压平为 `PROPAGATING_CANCELED`。超时身份的唯一载体是 token 状态机：
`originState()` 沿 parent 链找回根因，但只用于升级 `TIMEOUT` 归因。再深一层（成员内
嵌套 batch 自己的超时）对 group 只表现为成员失败——这是"谁拥有触发的 deadline，谁报
TIMEOUT"的有意封装。

一个实测修正（2026-09-04）：嵌套 batch 继承组 deadline 后自身 token 带有同一 deadline、
被 bind 并武装了自己的 timer，与"组→成员→嵌套"的传播级连同刻竞速，其终态可能是
`TIMEOUT_CANCELED`（自己的 timer 先触发）而非必然 `PROPAGATING_CANCELED`；两种终态经
`originState()` 都归因 timeout，行为确定正确。只有未 bind 的 token（成员 bind 跳过路径
下的成员 token）是例外——它只能被传播移动，终态确定为 `PROPAGATING_CANCELED`。
