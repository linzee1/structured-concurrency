# 增强 ListenableFuture 分析：现状增强盘点与公开增强类型评估

> 状态：**分析文档，待讨论**。回答"当前项目对 ListenableFuture 有哪些增强、可能需要哪些增强、
> 是否应默认返回增强型 LF（如 NamedListenableFuture）"。与
> [group-api-redesign-v0.3-proposal.md](group-api-redesign-v0.3-proposal.md) 相互独立又互补
> （见 §6）。事实部分均有源码行号支撑。

## 1. 现状：项目对 LF 的增强分两层，用户只见不到的那层在 future 上

### 1.1 机制层增强（在 future 上，但对用户隐藏）

全库唯一的任务 future 类型是包私有的 `ExecutionPhaseHintFuture`
（`ExecutionPhaseHintFuture.java:25`，`extends AbstractFuture<V> implements RunnableFuture<V>`
——"future 即任务"，取消可直达 worker 线程）。它在裸 future 之上携带：

| 增强 | 位置 | 消费者 |
|---|---|---|
| `ExecutionPhase` 五态状态机（含 `CANCELED_BEFORE_RUN` 取消竞态） | L35-61 | **只有 purge**：`GlobalPar.bindPurgeObserver`（`GlobalPar.java:358-364`）观察取消前未运行信号驱动 `HeuristicPurger` |
| `volatile Thread runner` + `interruptTask()` 精确中断 | L64, L166-172 | 内部（等价手工实现 `FutureTask.cancel(true)` 的 runner 语义） |
| `SubmissionException` 区分 `SUBMISSION_FAILURE` / `USER_FAILURE` | L212-219 | 包私有 `outcome()`，经 `FutureInspector`（`FutureInspector.java:28-45`）间接消费 |
| executor 拒绝时 CPU 任务 inline fallback | L98 `submitPrepared` | 内部 |

**这些增强用户完全碰不到**：类型包私有，`outcome()` 包私有，phase 无公开访问器。
用户手里的 `ListenableFuture` 引用实际就是这个对象，但只能当裸 Guava future 用。

### 1.2 语义层增强（用户需要的，全部不在 future 上）

用户拿到一个成员 future 后，以下信息**都无法从 future 本身得知**，只能走平行通道
（`TaskListener` 事件 / `TaskBatchResult.report()` / `TaskGroupResult` 快照）事后获取：

| 信息 | 现存放位置 | 用户可达途径 |
|---|---|---|
| 取消/失败归因（FAIL_FAST / TIMEOUT / GROUP_CANCELED / SUBMISSION_FAILURE…） | `CancellationToken.state()/originState()` + 归因表 `TokenOutcomes`（包私有） | 仅收敛后的 `report()` / `TaskCompletion.outcome()` |
| 任务名 | `MultiTaskContext.name()`（内核） | 仅 `TaskCompletion.taskName()` |
| deadline / 剩余预算 | `MultiTaskContext.deadlineNanos()/remaining()` | 组：`TaskGroupResult.deadlineNanos()`；批：无 |
| unitId / groupId / taskIndex / executorLabel | `MultiTaskContext` / `TaskExecutionContext`（内核） | 仅 `TaskCompletion` 各访问器 |
| enqueued / waitTime / executionTime | `TaskExecutionContext` 打点（`ScopedCallable`） | 仅 `TaskCompletion` |

这就是"功能不内聚"在 future 维度的表现：**机制增强埋在 future 里不给用，
语义增强绕开 future 走第二条通道**。`FutureInspector` 的 `instanceof` 分支
（从 future 反推 outcome）是这个裂缝的现存证据。

### 1.3 占位符通道（第三条、最裸的通道）

批窗口外任务是裸 `SettableFuture` 占位（`SlidingWindowSubmitter.java:88-90`），
`setFuture()` 桥接（L161）。占位符**无任何归属信息**（无名字/索引/unit），
被放弃时只有 `setException` / `cancel(true)` 两种无归因终态（L187-191），
且 submitter 对结果列表有两处 `(SettableFuture<V>)` 向下强转（L161, L187）。

## 2. 候选增强逐项评估

按 first-principles 判据（消除哪类"忘记/误用"？能否参数化现有机制？是否撑破抽象？），
对"public 增强 future 接口"的候选能力打分：

| 候选 | 价值判定 | 理由 |
|---|---|---|
| `taskName()` | **要** | `Checkpoints.checkpoint(taskName, …)` 与任务名的隐式约定（`Checkpoints.java:47-55`）从此可自检；日志/诊断不再需要外部映射表。prepare 时零成本 |
| `outcome()` 实时归因 | **要，核心价值** | 消除"用 `isCancelled()` 事后猜原因"这整类误用（设计契约明令禁止框架自己这么做，用户却没有替代手段）。token 在 `MultiTaskContext` 里早已解析好，`TokenOutcomes` 归因表现成。token 先 CAS 再取消的顺序保证终态后归因稳定 |
| `deadlineNanos()` / `remaining()` | **要（小）** | 嵌套任务做预算分配时需要；数据在 `MultiTaskContext` 现成 |
| 时间戳（submit/start/end、wait/execution） | 可选 | 与 `TaskCompletion` 重复；放上 future 等于承认 listener 不是唯一遥测口。倾向**不给**，保持快照通道唯一 |
| `taskIndex()` / `unitId()` / `groupId()` | 倾向不给 | 纯遥测，future 上收益低；`groupId` 还需回填（组对象晚于成员 future 创建，`TaskGroup.java:471` vs L554） |
| 链式编排（thenApply/exceptionally） | **不给** | graveyard 已否决（编排交给 Guava/CF），不翻案 |
| 暴露 `CancellationToken` 本体 | **不给** | token 是自助式强大机制；future 只暴露其派生的只读视图（outcome/deadline/remaining） |

命名判断：**`NamedListenableFuture` 低估了这次增强**——name 是价值最低的一项，
核心是"知道自己为何而终的 future"。建议 `TaskFuture<T> extends ListenableFuture<T>`，
与现有词汇（`TaskOutcome`/`TaskCompletion`/`TaskOptions`）同族，signature 草样：

```java
public interface TaskFuture<T> extends ListenableFuture<T> {
    String taskName();
    TaskOutcome outcome();          // 未终态返回 RUNNING；终态后归因稳定
    long deadlineNanos();           // 绝对 deadline（System.nanoTime 基准）
    Duration remaining();           // 剩余预算
    @Nullable Throwable failure();  // USER_FAILURE/SUBMISSION_FAILURE 时的 cause
}
```

`outcome()` 语义：未终态 → `RUNNING`；终态 → `FutureInspector` 路径
（成功 SUCCESS / 失败区分 SUBMISSION_FAILURE·USER_FAILURE / 取消读 token 归因含
`originState()` 传播链）。组级最终归因仍以收敛快照 `TaskGroupResult` 为准——
future 给的是"该成员视角的稳定归因"，二者不矛盾（成员视角正是 token 归因）。

## 3. 默认返回增强类型的影响面

替换点：`TaskBatchResult.results()` 元素、`TaskGroup` 成员 future、（v0.3 提案中）
`Builder.task()` 返回值、批占位符。

- **源码兼容**：声明返回类型从 `ListenableFuture<T>` 收窄为 `TaskFuture<T>` 对
  赋值给 `ListenableFuture<T>` 的调用方源码兼容（二进制不兼容，0.x 阶段可接受，
  写进迁移指南即可）。
- **组合生态零损**：`TaskFuture` 仍是 `ListenableFuture`，`Futures.allAsList`/
  `transform`/`addCallback` 全部照常工作——不违反公理 4（不另造平行宇宙，
  只是给现有宇宙加只读仪表）。
- **`FutureInspector` 是天然迁移接缝**：公开接口后它直接对接口编程，
  `instanceof ExecutionPhaseHintFuture` 分支退役。

## 4. 实现约束（内核事实）

1. **信息在创建点齐备**：`TaskSubmissions.prepare`（`TaskSubmissions.java:49-55`）入参即全量
   （name/index/context/token/unit/deadline）。唯一缺口：`ExecutionPhaseHintFuture`
   目前不持有 `TaskExecutionContext`（context 在 `ScopedCallable` 里），prepare 时多传
   一根引用，零运行时成本。
2. **`SettableFuture` 是 final**，占位符无法继承塞字段。出路：Guava
   `ForwardingListenableFuture` 委托壳（无新依赖），壳持 name/index/token 引用 +
   可交换 delegate（占位 → 桥接后真实 future）。桥接取消语义必须保留：cancel 壳 →
   落到当前 delegate；`setFuture` 对已取消占位的级联取消（design/cancellation-propagation.md §4）
   用委托重实现或保留占位本体为 delegate。
3. **两处强转改造**：`SlidingWindowSubmitter.java:161,187` 的 `(SettableFuture<V>)`
   强转改为壳的自述方法（`bind(realFuture)` / `abandon(...)`），顺手消掉
   "占位符终态无归因"问题——abandon 时壳知道自己是谁、为何而终。
4. **groupId 若上 future 需回填**，本评估已倾向不给，回填复杂度自然消失。

## 5. 风险与边界

- **语义承诺变重**：`outcome()` 一旦公开，归因表（`TokenOutcomes`）从内部实现细节升格为
  API 契约；取消竞态（`causedByCancellation`：checkpoint 异常赢了级联竞态）的归归类必须
  写进文档并测试。这是本提案最大的长期成本。
- **接口瘦身纪律**：每加一个访问器都要过"消除哪类误用"的检验；时间戳类已判定倾向不给，
  防止它长成第二个 `TaskCompletion`。
- 批占位符的壳化会让 `results()` 列表元素类型统一为 `TaskFuture`（窗口内外一致），
  顺带修复"窗口内外 future 实际类型不同"这一现状隐裂。

## 6. 与 v0.3 group 提案的协同

两个提案互相成就：

- v0.3 提案的"声明期返回占位 future"**必须**有 §4.2 的委托壳才能成立（占位符要携带
  声明期的名字与类型）；有了 `TaskFuture` 壳，`Builder.task()` 直接返回
  `TaskFuture<T>`，比返回裸 `ListenableFuture<T>` 更完整。
- `TaskFuture.taskName()` 让 v0.3 删掉的 `TaskKey` 的名字职能有着落：
  名字跟着 future 走，`Checkpoints` 的 taskName 约定可以自检。

建议的落地顺序：**先做 `TaskFuture`（本分析）→ 再做 v0.3 group 表面重构**，
后者可以直接站在增强 future 上，避免在裸 LF 上设计完再返工。

## 7. 结论

1. 项目对 LF 的增强现状是"机制增强藏在包私有 future 里、语义增强走平行通道"，
   公开增强类型的本质是把已有的内核信息**就地下放**到用户手里已有的对象上——
   不新增机制，只公开视图，完全符合判据 2（参数化现有机制而非新增概念）。
2. 增强项收敛为 4 个只读访问器：`taskName()` / `outcome()` / `deadlineNanos()` /
   `remaining()`（+ `failure()`）；名字建议 `TaskFuture<T>` 而非 `NamedListenableFuture`。
3. 唯一结构性工作是占位符委托壳（`SettableFuture` final）与两处强转改造，
   它们同时是 v0.3 group 提案的前置条件。
