# 独立并行任务组最终设计契约

> 本文是 `TaskGroup` 的独立实施规范。实现者只依赖本文和当前代码库即可完成开发，
> 不需要再参考早期草稿。文中的 MUST、MUST NOT、SHOULD 分别表示必须、禁止和推荐。

> **入口 API 已演进（0.2.x 快照）**：本文描述的 `GlobalPar.taskGroupBuilder()` /
> `ParallelTaskGroup.Builder.addTask()` / `buildAndSubmitAll()` / `TaskHandle` 配置流程已被替换为
> 不可变、可复用的 `TaskGroupSpec` 加一次性 `TaskGroup.submit(global, spec)` 和
> `TaskRef<T>` 令牌；选项类型统一为 `MultiTaskOptions`，timeout 必须显式二选一
> （`timeout(Duration)` 或 `inheritTimeout()`）。结构父任务、observation 与组 deadline 改在
> submit 时按提交线程解析（spec 不再在创建时捕获线程上下文），成员按注册名解析 executor，
> 不再校验 `Par` 归属。下文关于取消、收敛、deadline 与提交线性化的运行语义仍然有效；
> 涉及 Builder 生命周期（配置/消费状态机、`addTask` 校验时点）的段落描述的是被替换的历史设计。
> 当前 API 见 [使用指南](../user-guide.md) 与 [v0.2 迁移指南](../migration-v0.2.md)。

## 1. 目标与非目标

`TaskGroup` 用于在一个显式协调范围内提交少量、具名、类型可以不同、彼此没有数据依赖的任务：

```text
account-page group
├─ get-user       -> User
├─ get-orders     -> List<Order>
└─ get-inventory  -> Inventory
```

它提供：

- 通过 `TaskGroupSpec` 收集具名任务定义，并在唯一的 submit 时点统一冻结和提交；
- 组级 deadline、取消和默认 fail-fast；
- 每个成员独立选择已经注册的 `Par`；
- 类型安全的成员 `ListenableFuture<T>`；
- 所有冻结成员的最终收敛结果和组级 telemetry；
- 与现有单任务执行、取消、队列清理和任务监听能力复用。

它不提供：

- 任务依赖 DAG、结果到下一任务的自动传递；
- 重试、回退、配额或新的 executor；
- 列表批量展开或滑动窗口；
- 任意应用 `ThreadLocal`/MDC 的传播承诺；
- 一个隐式的“当前任务组” ThreadLocal。

列表处理继续使用 `Par.map()`。一个 Group member 永远代表一次 `Callable` 执行；如果该 callable 内部显式调用 `Par.map()`，那个 map 是成员创建的嵌套 Batch，不是 Group 自动展开的成员。

## 2. Group 与 Batch 的语义边界

| 维度 | Batch (`Par.map`) | Group (`TaskGroup`) |
|---|---|---|
| 业务含义 | 一个函数映射同类输入 | 多个异构操作共享协调范围 |
| 集合形成 | `map()` 调用时固定 | `TaskGroupSpec.Builder.task()` 配置，`TaskGroup.submit()` 冻结并统一提交 |
| 成员身份 | `taskIndex` | 唯一 `memberName` |
| 返回类型 | 全部为同一个 `R` | 每个成员可以有不同 `T` |
| executor | 整个 Batch 使用一个 `Par` | 每个成员选择自己的 `Par` |
| 调度 | `ConcurrentLimitExecutor` 滑动窗口 | build 时为全部成员准备后逐一独立提交 |
| 完成 | 固定 futures 全部终态 | build 时冻结的全部成员 future 终态 |
| 关系 | 嵌套 Batch 可以形成 TaskGraph 依赖边 | membership 本身不是依赖边 |

Group MUST NOT 通过 `Par.map(singletonList, ...)` 实现，也 MUST NOT 对外暴露 `AsyncBatchResult<Object>`。

## 3. 公共 API

公共类型放在 `io.github.huatalk.parallelinscope.scope`；监听 SPI 放在 `io.github.huatalk.parallelinscope.spi`。

### 3.1 创建与使用

当前入口（替代下文的 Builder 设计）：组由不可变、可复用的 `TaskGroupSpec` 描述，经一次性
`TaskGroup.submit(global, spec)` 冻结并统一提交：

```java
TaskGroupSpec.Builder spec = TaskGroupSpec.builder(
        MultiTaskOptions.of("account-page")
                .timeout(Duration.ofSeconds(3))
                .build());

TaskRef<User> user = spec.task(
            "get-user", "user", userService::getUser,
            MultiTaskOptions.of("get-user").inheritTimeout().build());
TaskRef<List<Order>> orders = spec.task(
            "get-orders", "order", orderService::getOrders,
            MultiTaskOptions.of("get-orders").inheritTimeout().build());
TaskRef<Inventory> inventory = spec.task(
            "get-inventory", "inventory", inventoryService::getInventory,
            MultiTaskOptions.of("get-inventory").inheritTimeout().build());

try (TaskGroup group = TaskGroup.submit(global, spec.build())) {
    User userValue = group.future(user).get();
    TaskGroupResult result = group.completionFuture().get();
}
```

`TaskGroupSpec` 是纯数据描述；`TaskGroup` 是 submit 后的运行对象。当前公共面：

```java
public final class TaskGroupSpec {
    public static Builder builder(MultiTaskOptions groupOptions);
    public MultiTaskOptions groupOptions();
    public List<MemberSpec<?>> members();

    public static final class Builder {
        public <T> TaskRef<T> task(
                String memberName,
                String executorName,
                Callable<T> callable,
                MultiTaskOptions options);
        public TaskGroupSpec build();
    }
}

public final class TaskRef<T> {
    public String memberName();
}

public final class TaskGroup implements AutoCloseable {
    public static TaskGroup submit(GlobalPar env, TaskGroupSpec spec);

    public String groupId();
    public String groupName();
    public void cancel();
    public ListenableFuture<TaskGroupResult> completionFuture();

    public Optional<ListenableFuture<?>> findMember(String memberName);
    public Map<String, ListenableFuture<?>> members();
    public <T> ListenableFuture<T> future(TaskRef<T> ref);

    @Override
    public void close();
}
```

语义：

- `TaskGroupSpec.Builder.task()` 只校验并保存不可变任务定义（memberName 为空或重复、参数为
  null 立即拒绝）；不得提交 executor、启动 timer、创建
  `MultiTaskContext`/`TaskExecutionContext` 或占用运行期资源；
- `TaskGroup.submit()` 是唯一的冻结与提交入口；它按提交线程解析结构父任务与
  observation、创建并注册全部成员后才允许任何成员进入 executor；spec 本身可重复提交；
- `TaskRef<T>` 是配置期发放的类型化令牌，不携带执行状态；`group.future(ref)` 在组内解析成员
  future，引用不属于该组的 memberName 时抛 `IllegalArgumentException`；
- `cancel()` 幂等、非阻塞，固定 `CANCELED`（若尚未固定）并取消未完成成员；
- `close()` 是异常安全清理：若仍有未完成成员，语义等同 `cancel()`；若所有成员已经终态或空组则无副作用；
- `completionFuture()` 在全部冻结成员的公开 future 终态后完成，不存在另行封口条件；
- `members()` 返回按定义顺序排列、不可修改的完整集合；
- `findMember()`/`members()` 在 Group 返回给调用方时即可看见全部冻结成员。

`TaskRef<T>` 取代早期嵌套 `TaskHandle<T>`：异构任务的 future 在统一 submit 时创建，调用方用
配置期拿到的令牌在提交后取回类型安全的 future。spec 不捕获线程上下文，因此结构归属始终由
提交现场决定。

### 3.2 组级与成员级选项

组级与成员级选项统一为 `MultiTaskOptions`；组读取 name/timeout/listeners，成员读取
name/parallelism/timeout/taskType/rejectEnqueue：

```java
public final class MultiTaskOptions {
    public static Builder builder();
    public static Builder of(String name);

    public String name();
    public Optional<Duration> timeout();
    public List<TaskGroupListener> listeners();
}
```

- `name` 非空；
- timeout 必须显式二选一：`timeout(Duration)`（正数）或 `inheritTimeout()`（继承外层
  deadline）；两者都未声明或同时声明时 `build()` 抛 `IllegalArgumentException`；`timeout()`
  访问器返回空 `Optional` 表示继承；
- 组级 `inheritTimeout()` 要求 submit 时存在外层 scoped task，否则 `submit` 抛
  `IllegalArgumentException`；成员级 `inheritTimeout()` 解析为组 deadline；
- options 不保存运行状态，可安全复用；
- listener 在 options 构建时复制成不可修改快照，submit 时使用该快照。

### 3.3 结果类型

```java
public enum TaskGroupCompletionReason {
    SUCCESS,
    FAILED,
    TIMEOUT,
    CANCELED
}

public enum TaskGroupMemberReason {
    SUCCESS,
    USER_FAILURE,
    SUBMISSION_FAILURE,
    MEMBER_CANCELED,
    GROUP_CANCELED,
    FAIL_FAST,
    TIMEOUT
}
```

`TaskGroupResult` 和成员结果必须是完成后的不可变快照：

```java
public final class TaskGroupResult {
    public String groupId();
    public String groupName();
    public long startTimeNanos();
    public long endTimeNanos();
    public long deadlineNanos();
    public TaskGroupCompletionReason completionReason();
    public @Nullable String failedMemberName();
    public Map<String, TaskGroupMemberResult> members();
}

public final class TaskGroupMemberResult {
    public String memberName();
    public TaskGroupMemberReason completionReason();
    public @Nullable Throwable failure();
    public TaskContext taskContext();
}
```

要求：

- Map 按任务定义顺序稳定输出且不可修改；
- `failure` 仅用于 `USER_FAILURE` 和 `SUBMISSION_FAILURE`；
- 结果保存完成原因，MUST NOT 仅根据 `Future.isCancelled()` 反推原因；
- `TaskContext` 是完成后只读视图；Group result 可以持有它，但不得再把它安装为 current task；
- `completionFuture()` 正常完成并返回 `TaskGroupResult`，组的 `FAILED/TIMEOUT/CANCELED` 是结果数据，不通过 completion future 本身抛错表达；
- 单个成员 future 保持普通 Guava 语义：成功返回值、失败抛 `ExecutionException`、取消表现为 cancelled。

## 4. 对象与上下文生命周期

### 4.1 总览

```text
应用生命周期
GlobalPar ────────────────────────────────────────────────────────────

配置生命周期
TaskGroupSpec.Builder ── task* ── build ── TaskGroupSpec（不可变、可复用）

请求/显式协调生命周期
TaskGroup ── created/running ── all terminal ── closed

每个成员对象生命周期
MemberSpec ── added ── frozen ─┐
MemberState ──────────────────────── prepared/submitted/running ── terminal
MultiTaskContext ────────────────────────────────────────────────
TaskExecutionContext ─ created ─ queued ─ run ─ completed/event ─────

动态线程绑定
SubmissionScope                    └─ executor.execute(...) ─┘
TaskExecutionContext.current()                 └─ user callable ─┘

请求级观测（可选）
TaskGraphObservationContext ─────────────────────────────────────────
```

### 4.2 TaskGroup

`TaskGroupSpec` 只保存组级 options 和有序成员定义，是纯数据、可复用对象；外层上下文、
observation 和 deadline 归属在每次 `TaskGroup.submit()` 时按提交线程解析。成员定义
（`MemberSpec`）是 spec 的不可变组成部分，不新增公共 Context。spec 不属于任何物理线程。

`TaskGroup` 本身就是组级运行状态，MUST NOT 再新增 `TaskGroupContext` 或 `CurrentTaskGroupTl`。

它在 `TaskGroup.submit()` 的运行期创建阶段产生，至少持有：

```text
groupId / groupName
startTimeNanos / deadlineNanos
lifecycle
first completion reason
frozen members registry
memberCount / terminalCount
group CancellationToken（内部，不作为公共控制入口）
completion SettableFuture
failedMemberName
TaskGraphObservationContext snapshot（可空）
listener snapshot
```

其中 `first completion reason` 接受 member failure、deadline、group cancel/close、outer cancellation、成员被直接取消。单个成员被调用方直接取消会级联取消整个 Group（见 8.2）。Group 对象在调用方、成员完成 listener 或 completion future 仍引用它时继续存活；它不属于任何物理线程。

### 4.3 MemberState

每个冻结成员需要一个内部状态记录。推荐作为 `TaskGroup` 的私有/包可见内部类，避免新增公共 Context：

```text
memberName
TaskExecutionContext
公开 ListenableFuture
执行 future/phase
原子 MemberCompletionReason
failure（可空）
```

生命周期从 build 的全量注册阶段开始，到 Group result 不再被引用为止。成员可能在用户函数开始前取消，此时 MemberState 存在，但 `TaskExecutionContext` 从未安装，且不得伪造 `TaskListener.TaskEvent`。

### 4.4 MultiTaskContext

每个成员创建一个单任务 `MultiTaskContext`：

```text
taskCount = 1
effectiveParallelism = 1
taskName = member MultiTaskOptions.name
executorIdentity / parLabel = member Par 的绑定
```

`memberName` 与 `taskName` 不重复承担同一职责：

- `memberName`：Group 内唯一键和组级结果键；
- `taskName`：现有任务执行、checkpoint 和 TaskListener 的诊断名称。

成员 options 中的 `parallelism` 不产生多个执行实例；解析结果 MUST 固定为 1。

### 4.5 TaskExecutionContext

每个成员在 build 时创建一个 `TaskExecutionContext(batch, 0, submitTimeNanos)`。所有成员的 `submitTimeNanos` SHOULD 使用同一个 build 提交基准时间，避免提交循环顺序改变组内计时口径。对象从准备阶段存在，但只在 `ScopedCallable.call()` 的用户任务执行阶段安装到普通 ThreadLocal：

```text
install member task
  markStarted
  checkpoint
  user callable
  markEnded
clear current task
  TaskListener callback（显式读取 TaskEvent）
restore previous task
```

这保证 inline/synchronous fallback 下的嵌套恢复：

```text
outer current -> member current -> outer current
```

TaskListener 回调期间 `TaskExecutionContext.current()` MUST 为 null，避免 listener 提交的新任务被误判为已完成成员的结构化子任务。

### 4.6 SubmissionScope

成员每次真正调用目标 executor 的 `execute/submit` 时，必须临时安装成员 Batch：

```java
MultiTaskContext previous = SubmissionScope.install(memberBatch);
try {
    executor.execute(memberFuture);
} finally {
    SubmissionScope.restore(previous);
}
```

它的生命周期只覆盖一次提交调用，不覆盖排队或任务执行。必须保留栈式恢复，以支持拒绝后 inline 执行期间再次嵌套提交的情况。

### 4.7 TaskGraphObservationContext

`submit()` 解析提交线程上同一个 `GlobalPar` 当前有效的 observation；不存在或 owner 不匹配时按 null 处理。成员执行必须使用该解析结果，而不是把 Group membership 写成 TaskGraph edge。

调用方必须让 observation 生命周期覆盖 Group 的所有成员执行。若 observation 已提前关闭，成员不得复活它，后续图记录可以安全忽略。

实现可在创建 `TtlCallable` 快照时短暂 install/restore Group 捕获的 observation，或者为单任务提交 helper 提供显式 observation 参数；MUST NOT 新增 group TTL。

### 4.8 TTL 边界

成员仍使用 `TtlCallable.get(callable, true, true)`，使提交时应用已有的 `TransmittableThreadLocal` 按 TTL 规则捕获和恢复。框架不承诺传播普通 `ThreadLocal` 或自动配置 MDC。排队任务会持有 TTL 快照，这是 TTL 本身的 retention 语义。

## 5. 结构化 parent、取消 parent 与 deadline 上限必须解耦

现有 `MultiTaskContext.resolve()` 把三件事都从 `parent MultiTaskContext` 推导：

1. TaskGraph/嵌套结构 parent；
2. cancellation token parent；
3. deadline ceiling。

Group member 证明这三者不总是同一个对象。实现 MUST 抽取一个新的解析重载或内部 factory，使其可独立传入：

```java
MultiTaskContext resolve(
        MultiTaskOptions options,
        int taskCount,
        @Nullable MultiTaskContext structuralParent,
        @Nullable CancellationToken cancellationParent,
        long deadlineCeilingNanos,
        @Nullable TaskGraphObservationContext observation,
        ExecutorIdentity executorIdentity,
        String parLabel);
```

具体方法名可以不同，但语义不可合并回虚假 parent。

### 5.1 Group 在普通请求线程创建

```text
group structural parent = null
group token parent       = null
group deadline           = build time + group timeout

member structural parent = null
member token parent       = group token
member deadline           = min(member requested deadline, group deadline)
```

### 5.2 Group 在一个正在执行的 scoped task 中创建

`TaskGroup.submit()` 按提交线程解析当时的 `TaskExecutionContext.current()` 作为结构父任务；同一个 spec 无论之后从哪个线程提交，归属都由该次提交现场决定：

```text
outerBatch = currentTask.batchContext()

group token parent        = outerBatch.cancellationToken
group deadline            = min(requested group deadline, outerBatch.deadline)

member structural parent  = outerBatch
member token parent        = group token
member deadline            = min(requested member deadline, group deadline)
```

Group 本身不是一个虚构 Batch，也不创建 Group TaskGraph node。每个 member 与 `outerBatch` 之间可以记录真实的 outer-to-member 依赖边；members 之间不得产生边。若 Group 在请求线程创建，则没有这些边。

外层取消必须使 Group 首次原因固定为 `CANCELED`（若失败/超时尚未先赢），并取消成员。为此可为 `CancellationToken` 增加包可见的完成监听能力；不得靠轮询。

## 6. 状态机与完成条件

`TaskGroupSpec` 是不可变纯数据，不存在配置/消费状态机；只有运行 Group 有生命周期：

```text
Group:   RUNNING -----all members terminal--> CLOSED
```

- `TaskGroupSpec.Builder` 非线程安全，调用方必须在一个配置流程中完成定义后 `build()`；build 出的
  spec 可安全共享并重复提交；
- 每次 `TaskGroup.submit()` 独立创建运行对象；spec 没有"已消费"状态；
- 返回的 Group 从一开始就持有完整、不可扩展的成员集合；不存在 `OPEN`、`SEALED` 或 `ACTIVE`；
- `CLOSED` 只表示全部公开成员 future 已终态且不可变结果已经发布。

完成原因单独记录，并遵循 first-wins：

```text
null --first member failure--> FAILED
null --deadline-------------> TIMEOUT
null --group cancel/close/parent--> CANCELED
null --all success-----------> SUCCESS
null --member canceled + final convergence--> CANCELED
```

规则：

- `FAILED/TIMEOUT/CANCELED` 一旦 CAS 成功，后续事件不得覆盖；
- 非成功原因会取消其他未完成成员；
- `SUCCESS` 只有在所有冻结成员均成功时才能固定；
- 单个成员被调用方直接取消时只记录一个 `CANCELED` 候选，不立即固定 Group 原因；这样 build 提交循环中随后发生的真实 submission failure，或最终收敛前发生的 timeout，仍能固定为 `FAILED/TIMEOUT`；
- 全部成员终态且存在直接取消成员时，若没有更早的组级失败/超时，Group 原因固定为 `CANCELED`；
- `CLOSED` 只在 `terminalCount == memberCount` 时发布；
- Group 原因可以先固定，但 completion future 仍必须等所有公开成员 future 达到终态；
- 空 spec submit 后返回立即以 `SUCCESS` 完成的 Group，不启动物理 deadline timer；

建议 Group registry 在发布前构造完成，此后只读；完成原因和计数转换使用原子操作或一把私有 lock。成员 future 的完成 callback 在 lock 内只更新小型状态和决定后续动作，取消 future、触发 listener 等外部调用必须在 lock 外执行，防止重入和长时间占锁。

## 7. submit、冻结与统一提交契约

### 7.1 配置期校验

`TaskGroupSpec.Builder.task()` 应尽早拒绝以下定义错误，且不得产生任何运行状态：

- memberName 为空或重复；
- 参数为 null。

成员 executor 按注册名在 `submit()` 时经 `GlobalPar.par(executorName)` 解析，未知名称抛出
`IllegalArgumentException`。`task()` 不检查或消耗 deadline，因为 Group 的逻辑执行时间从
submit 开始。

executor rejection 只有实际提交时才能知道，因此属于 submit 后的成员运行结果，不是 spec 校验失败。若成员按现有 CPU-bound 策略在 rejection 后成功 inline 执行，则它是正常执行路径；只有配置的全部提交/回退路径最终失败时，公开 future 才以 `SUBMISSION_FAILURE` 终态并触发 Group fail-fast。

### 7.2 submit 线性化与步骤

`TaskGroup.submit()` 必须作为一次整体 admission 与 `GlobalPar.close()` 线性化，不能按成员分别跨越关闭边界。推荐让下列准备和注册阶段整体处于一次 `GlobalPar.whileOpen()` 中；实际 executor 调用仍须在内部锁和 GlobalPar admission lock 外进行：

本文所称“统一提交”是指所有成员共享一个逻辑 submission boundary：submit 前没有任何运行状态或执行，submit 时一次性冻结完整集合并使用同一个提交基准时间。它不表示对多个不同 executor 的 `execute()` 做物理原子广播；这些调用必然有先后，但只能在全部成员完成准备和注册后开始。

必须满足：

1. 在 `GlobalPar.whileOpen()` 内解析结构父任务/observation、按注册名解析成员 executor 并冻结有序任务定义；
2. 读取统一的 `startTimeNanos`，解析 Group deadline，并创建 Group token/运行对象；
3. 为每个定义创建 member Batch、TaskExecutionContext、公开 future 和执行权竞争对象；
4. 将全部 `MemberState` 注册到 Group，发布完整 members registry；
5. 空组立即发布 `SUCCESS` 并返回，不创建 timer；非空组安排 Group deadline timer；
6. 退出所有 registry/admission lock；
7. 按定义顺序向各自目标 executor 提交同一个 prepared future；
8. 提交循环结束后返回 Group；若某个成员 inline 执行、失败或触发 fail-fast，剩余尚未调用 executor 的 prepared future 也必须被取消并达到终态。

不能在全部成员注册前调用任何 `executor.execute()`，否则 direct executor 或 rejection fallback 可能在 Group 看见完整成员集合前执行用户代码。

不能为了避免该竞态而在持有 Group lock 时调用 `executor.execute()`；executor 可能 inline 执行任意用户代码，导致 close/cancel 长时间无法取得锁。

`TaskGroup.submit()` 正常返回时必须保证完整 members registry 已发布（`future(TaskRef)`
可立即解析），并且每个仍未因 fail-fast/timeout/cancel 终结的成员都已经尝试过一次目标 executor 提交。由于 direct executor 可以 inline 执行，返回时部分甚至全部成员已经终态属于合法行为。

成功跨过全量注册后，单个 executor rejection、inline 用户异常或 fail-fast 均通过成员 future 和 `TaskGroupResult` 表达，`submit()` SHOULD 仍返回 Group，而不是因任务运行结果抛异常。只有定义校验、GlobalPar 已关闭，或无法建立完整运行对象的框架级准备错误才允许 submit 直接抛出；此时必须终结已创建的 future、释放 retain/timer 等资源，并且不得执行任何用户 callable。

### 7.3 Prepared single-task submission

当前 `ListenableCompletionService.submit()` 把 future 创建和 `executor.execute()` 合在一起，不足以实现上述“全量注册后统一提交”。必须抽取一个内部、可复用的两阶段单任务提交内核，例如：

```java
PreparedScopedTask<T> prepared = singleTaskSubmitter.prepare(...);
// prepared.future() 已存在，但尚未交给 executor

registerAll(preparedTasks); // 所有 future 同时成为完整冻结集合

prepared.submit(); // executor.execute outside group lock
```

名称可以不同，但必须保证：

- `future()` 是对外返回、参与执行权竞争和 phase 观测的同一个逻辑 future；
- public future 必须能在执行 delegate cancel 前原子标记“调用方直接取消”，以区别 Group 传播取消、fail-fast 和 timeout；可以使用自定义 forwarding future，但不能只在完成 callback 中看到 `isCancelled()` 后猜测来源；
- `cancel()` 在线程取得执行权前成功后，之后的 prepared submission 不得进入用户 callable；
- prepared submission 被 executor 拒绝时，可以把 future 完成为 submission failure，不能遗留 pending future；
- 用户 callable 最多执行一次；
- phase 继续区分 `CANCELLED_BEFORE_RUN` 和 `CANCEL_REQUESTED_RUNNING`；
- `SubmissionScope` 只包住实际 `executor.execute()`；
- 支持现有 CPU-bound rejection 后 inline 执行策略，但 inline 也必须遵守已注册和执行权竞态；
- 每个冻结 future 最终达到终态。

该内核同时供 `Par.map()` 和 Group 使用，避免两套取消/phase/TTL/ScopedCallable 实现。Batch 仍在其上保留 `ConcurrentLimitExecutor` 的滑动窗口，Group 不使用滑动窗口。

## 8. 取消、deadline 与 fail-fast

### 8.1 Token 结构

不新增公共 `GroupCancellationToken`。Group 内部复用现有 `CancellationToken`，公共控制入口是 `group.cancel()`：

```text
outer task token（可空）
└─ group token
   ├─ member A token
   ├─ member B token
   └─ member C token
```

成员 deadline 先于 Group deadline 到期时，成员 token 上的 timeout 监听器必须把超时升级为 `groupToken.timeoutCancel()`，使 Group 收敛为 `TIMEOUT` 而不是 fail-fast 的 `FAILED`。

### 8.2 成员主动取消

调用方对成员 future（或成员 token）调用 `cancel()`：

- Group 取消语义与 batch 完全一致（结构化并发）：成员被直接取消即级联取消整个 Group；
- 该成员原因记录为 `MEMBER_CANCELED`；
- 未完成 siblings 通过各自 member token 级联取消，记录 `GROUP_CANCELED`；
- Group completion reason 固定为 `CANCELED`；
- 取消在线程取得执行权前获胜时，用户 callable 不得执行；
- 取消在 RUNNING 后获胜时发出中断请求，但不保证用户代码立即停止。

### 8.3 Fail-fast

任一成员出现 `USER_FAILURE` 或 `SUBMISSION_FAILURE`：

```text
CAS group reason FAILED
record failedMemberName（仅 first winner）
cancel every other unfinished member
wait every frozen public future terminal
publish CLOSED/result/event
```

被传播取消的 siblings 记录 `FAIL_FAST`，不能仅显示为笼统 cancelled。

### 8.4 Deadline

逻辑 deadline 在 `submit()` 时计算；spec 配置耗时不计入 Group timeout：

```text
requestedGroupDeadline = buildStartTicker + resolvedGroupTimeout
groupDeadline = outerBatch == null
        ? requestedGroupDeadline
        : min(requestedGroupDeadline, outerBatch.deadlineNanos())
```

非空组在全部成员准备并注册后、提交循环开始前安排一个物理 timer；空组不分配 timer。若外层 deadline 在 build 准备期间已经到期，则 Group 固定 `TIMEOUT`，全部已注册成员记录 `TIMEOUT` 并取消，且不得进入用户 callable。

timer 触发时：

- first-wins 固定 `TIMEOUT`；
- 未完成成员取消并记录 `TIMEOUT`；
- timer 必须在 Group 先完成时取消或成为无害 no-op；
- timeout action 使用 `GlobalPar.timeoutScheduler()`，Group 不创建 scheduler。

成员 deadline：

```text
memberDeadline = min(member requested/default deadline, groupDeadline)
```

若成员自己的 deadline 先到并导致该成员失败/取消，Group 应固定 `TIMEOUT`，因为结果 API 已明确区分 timeout；不得把它误报成普通 user failure。

现实现（0.2.x）：deadline 存储在 `CancellationToken` 内部（构造时与 parent 取 min），`bind(List, submitCanceller, timer)` 不再接收 Duration。Group 侧 `start()` 做三件事：先把每个成员的完成 observer 挂上，再对 group token 一次 `bind`（组 deadline + 统一 fail-fast + 全成功），最后对每个成员 token `bind` 自己的 future，并在成员 token 上注册状态监听器（`TIMEOUT_CANCELED` 时调用 `groupToken.timeoutCancel()`，监听器在 CAS 之后、取消动作之前同步触发）。

成员取消原因不再由发起取消处手写，而是收敛后读 token state：成员 token `TIMEOUT_CANCELED` 即 `TIMEOUT`；否则 group token 是唯一权威（它在取消成员 futures 之前先提交自己的状态），`TIMEOUT_CANCELED/FAIL_FAST_CANCELED/MUTUAL/PROPAGATING` 分别映射 `TIMEOUT/FAIL_FAST/GROUP_CANCELED`；两个 token 都仍是 `RUNNING` 说明没有框架路径碰过该成员，即用户直消，记 `MEMBER_CANCELED`。

### 8.5 close

`close()` 不阻塞：

- 空组或所有冻结成员已终态：无副作用；
- 存在未完成成员：等同 `cancel()`；
- 已 `CLOSED`：幂等；
- 不关闭 `GlobalPar` 或任何注册 executor。

## 9. 单任务运行内核的复用边界

### 9.1 必须复用

| 现有能力 | Group 中的用途 |
|---|---|
| `GlobalPar.whileOpen()` | 整体 build 与 shutdown 的线性化 |
| `GlobalPar.timeoutScheduler()` | Group/member deadline |
| `GlobalPar.retainUntilComplete()` | 冻结成员完成前保留内部服务 |
| `Par`/`ExecutorRuntime` | executor、identity、label、blocking risk、phase observer |
| `ScopedCallable` | current task、checkpoint、计时、TaskListener、恢复 |
| `TaskExecutionContext` | 单成员任务执行身份与 timing |
| `SubmissionScope` | 一次 executor submission 的队列策略 |
| `ExecutionPhaseHintFuture` | run/cancel 执行权竞态与 purge phase |
| `CancellationToken` | outer→group→member 取消传播和中断 |
| `HeuristicPurger` | 取消排队任务后的有界队列清理 |
| `TtlCallable` | 已配置 TTL 的提交时快照与恢复 |
| `TaskGraphObservationContext` | 请求级观测归属 |

### 9.2 不得复用

- `ConcurrentLimitExecutor`：Group 不使用滑动窗口、placeholder 或 completion queue 驱动提交；
- `AsyncBatchResult`：Group 是异构成员和固定的具名集合；
- `Par.map(singletonList, ...)`：会引入错误抽象和不必要包装；
- 虚构的 Group `MultiTaskContext`：membership 不是 Batch；
- Group ThreadLocal/TTL：Group 由显式对象持有，不是线程隐式状态。

### 9.3 建议代码组织

推荐新增/调整：

```text
scope/
  TaskGroup.java
  TaskGroupSpec.java
  TaskRef.java
  TaskGroupResult.java
  TaskGroupCompletionReason.java
  TaskGroupMemberReason.java

spi/
  TaskGroupListener.java

internal/
  SingleTaskSubmitter.java        // 或语义等价 helper
  PreparedScopedTask.java         // 可作为 SingleTaskSubmitter 内部类型
```

`ExecutorRuntime` 当前是 `scope` 包私有类型，`internal.SingleTaskSubmitter` MUST NOT 直接依赖或公开它。推荐由 `Par` 新增包可见的单任务准备入口（例如 `prepareSingleTask(...)`），在 `scope` 包内完成 owner、policy、runtime identity、executor 和 phase observer 的解析，再把普通参数传给 internal kernel。`TaskGroup` 与 `Par` 同包，可调用该入口；公共 API 不暴露 runtime。

共享内核至少分离以下阶段：

```text
Par/package-private entry
  ├─ validate owner and resolve member MultiTaskContext
  ├─ create TaskExecutionContext + ScopedCallable + TTL wrapper
  └─ internal kernel prepare execution future

Group
  ├─ register prepared future (linearization)
  └─ invoke prepared submission outside Group lock
```

Batch 路径可以继续由 `ConcurrentLimitExecutor` 组织滑动窗口，但其 future 创建、phase claim、SubmissionScope、rejection/inline 和 scoped callable 包装应下沉到相同的低层组件。不要为 Group 单独复制 `ScopedCallable`、execution phase future 或 cancellation token。

## 10. TaskListener 与 Group telemetry

### 10.1 成员级 TaskListener

真正进入 `ScopedCallable.call()` 的成员继续触发现有 `TaskListener.TaskEvent`：

- 成功结果、用户异常；
- submit/start/end timing；
- queue wait 分类；
- `TaskContext` 和 Batch taskName。

执行前取消或提交失败的成员没有真实 start/end，不得伪造 TaskEvent。它们必须在 `TaskGroupResult` 和 Group listener 中可见。

Group 通过 MemberState 中保存的 `TaskExecutionContext` 身份把 memberName 与 TaskEvent/结果关联，不需要新增 current group context。首版不要求修改 `TaskContext` 增加 groupId/memberName。

### 10.2 TaskGroupListener

```java
@FunctionalInterface
public interface TaskGroupListener {
    void onTaskGroupComplete(TaskGroupEvent event);
}
```

`TaskGroupEvent` 可以直接包装不可变 `TaskGroupResult`，或暴露等价字段。它只在 CLOSED 后调用一次。

要求：

- listener 异常隔离并通过 JUL 记录；
- 不改变 completion result；
- listener 回调期间不得安装任何 member current task 或 group current context；
- 回调不在 Group lock 内执行；
- completion future 应先固定还是 listener 先执行必须选择并测试；推荐先固定 result/completion future，再调用 listener，避免 listener 阻塞调用方观察终态；
- 文档要求 listener 非阻塞并容忍并发，但框架仍必须保护自身状态。

## 11. TaskGraph 规则

Group membership 不是依赖关系：

```text
group
├─ member A
├─ member B
└─ member C
```

MUST NOT 产生：

```text
A -> B
B -> C
fake-group-batch -> A/B/C
```

规则：

- 请求线程创建的 Group，其 members 不因同组而写 TaskGraph edge；
- scoped task 内创建的 Group，可以为真实 outer batch 到各 member batch 写边；
- member callable 内部调用 `Par.map()` 时，现有 `TaskExecutionContext.current()` 使该 Batch 成为 member 的真实结构化 child；
- groupId/memberName 的 membership 只写入 Group telemetry；
- 相同物理 executor 的成员不会仅因同组产生 self-loop/deadlock 告警。

## 12. GlobalPar 关闭与资源所有权

- `TaskGroupSpec` 是纯配置对象，创建它不验证 GlobalPar 状态，也不长期 retain；
- `TaskGroup.submit()` 的冻结、运行对象创建和全量成员 admission 必须整体通过一次 `GlobalPar.whileOpen()`，使 submit 要么在线性化点先于 close 接纳完整组，要么完整拒绝；不得出现只接纳一部分成员；
- submit 完成 admission 后，即使 GlobalPar 随后关闭，冻结成员也必须完成取消、timeout、listener 和结果收敛；
- 每个冻结成员通过 `retainUntilComplete()` 计入活动运行；可以增加 group-aware retain helper，但不得提前关闭 timer/submitter/maintenance 服务；
- Group 不创建或关闭业务 executor；
- `GlobalPar.close()` 不阻塞，不关闭注册 executor；
- Group deadline 使用 GlobalPar 拥有的 scheduler。

## 13. 并发不变量

实现和测试必须证明：

1. 每个 memberName 在一个 `TaskGroupSpec` 中至多定义一次；
2. 每次 submit 中每个 callable 最多执行一次；
3. spec 不可变、可复用；每次 submit 冻结的是同一份完整定义；
4. Group 一旦发布，其 registry 完整、不可扩展，且每个成员都拥有一个最终终态的公开 future；
5. executor rejection 不能留下 pending future；
6. `terminalCount` 对每个成员最多增加一次；
7. Group completion reason 只固定一次；
8. Group listener 只调用一次；
9. completion future 只在全部冻结成员终态后完成；
10. 取消赢得 execution claim 时用户 callable 不执行；
11. 任一 inline 执行前，全部成员已经出现在 registry；
12. 所有 ThreadLocal/TTL 在正常、异常、取消、拒绝和 inline 路径上恢复；
13. Group lock 内不执行用户 callable、listener、future cancellation 或 executor 方法；
14. 同一成员的公开 future 状态、MemberReason、Group result 三者一致。

## 14. 必测矩阵

### 14.1 基本行为

1. 三个异构成员分别返回不同类型且各执行一次；
2. 成员使用不同 `Par`/executor 时并行运行；
3. 同一 `Par` 上多个成员均独立提交，不经过滑动窗口；
4. 重复 memberName、空名称、null 参数在 `task()` 配置期拒绝；未知 executorName 在 submit 时拒绝且没有 callable 执行；
5. `task()` 不创建执行上下文、不启动 timer、不提交或执行 callable；
6. 空 spec submit 返回立即 SUCCESS 的 Group，未创建 timer；`TaskRef` 在 submit 后即可经 `group.future(ref)` 稳定解析。

### 14.2 submit 与关闭竞态

7. 同一个 spec 可重复 submit，每次产生独立 Group（不同 groupId）；
8. `TaskGroup.submit()` 与 `GlobalPar.close()` 竞争时，要么完整组被接纳，要么 submit 完整拒绝，绝无部分 admission；
9. direct executor/inline fallback 中，任一 callable 执行前完整成员集合已可查询；
10. executor rejection 产生 SUBMISSION_FAILURE、触发 fail-fast、所有 future 终态；
11. 已注册但尚未 `executor.execute()` 的成员被 fail-fast/cancel 后不得运行 callable；
12. 重复 cancel、close 不重复计数或回调；submit 提交循环中的 inline failure 会终结尚未提交成员。

### 14.3 取消与原因

13. 手动 group.cancel 使未完成成员记录 GROUP_CANCELED；
14. 直接 member future.cancel 级联取消 Group：该成员 `MEMBER_CANCELED`，未完成 siblings `GROUP_CANCELED`，Group reason `CANCELED`；
15. 成员失败固定 FAILED，first failedMemberName 稳定，siblings 为 FAIL_FAST；
16. 取消发生在 SUBMITTED/RUNNING/TERMINAL 三个阶段时状态一致；
17. 用户忽略中断时公开 future 可以先取消，但 Group 仍能按公开 future 终态完成；
18. outer scoped task 取消向 group/member 传播且不反向影响 outer。

### 14.4 deadline

19. group deadline 早于 member deadline，Group TIMEOUT，成员 TIMEOUT；
20. member deadline 早于 group deadline，该 member TIMEOUT 并使 Group TIMEOUT；
21. failure、timeout、cancel 同时竞争时 first-wins 且不覆盖；
22. 外层 deadline 在 submit 准备期间过期：所有冻结成员不执行，Group TIMEOUT；
23. Group 提前完成后 timer 取消或触发为 no-op。

### 14.5 上下文与监控

24. 每个运行成员看到自己的 `TaskExecutionContext.current()`；执行后恢复 previous/null；
25. inline 嵌套执行呈现 outer -> member -> outer；
26. `SubmissionScope` 仅覆盖 executor submission，并在 rejection/inline/异常后恢复；
27. TaskListener 中 current task 为 null，TaskEvent 指向正确成员 TaskContext；
28. 执行前取消和 submission failure 不产生虚假 TaskEvent；
29. Group listener 只调用一次，异常不改变结果；
30. TTL 值以 `submit()` 的 prepare 阶段为捕获时点传播并恢复；`task()` 时的值不构成快照，普通 ThreadLocal 不承诺传播。

### 14.6 TaskGraph 与关闭

31. 请求线程 Group 不产生 membership dependency edge；
32. outer scoped task 创建 Group 时，只产生 outer->member 的真实边；
33. member 内嵌套 `Par.map()` 产生 member->child Batch 的真实边；
34. 同一 executor 的 siblings 不因 membership 产生 self-loop；
35. GlobalPar close 后拒绝新 submit；先于 close 完成 admission 的完整 Group 可继续运行；
36. close 前完整冻结的成员继续走终止、timeout 和 telemetry；
37. Group close 不关闭任何注册 executor。

## 15. 实施顺序

推荐按以下顺序实现，每一步保持测试可运行：

1. 解耦 `MultiTaskContext` 的 structural parent、cancellation parent 和 deadline ceiling，并保持现有 `Par.map()` 行为不变；
2. 抽取两阶段单任务提交内核，先让 `Par.map()` 或针对 helper 的测试证明 TTL、SubmissionScope、phase、rejection 和 inline 语义；
3. 实现组级/成员级 `MultiTaskOptions` 三态 timeout、结果枚举和不可变结果对象；
4. 实现 `TaskGroupSpec`、`TaskRef<T>`、一次性 `submit`、全量冻结 registry、计数和 completion future；
5. 接入 group/member token、deadline、fail-fast 和 outer cancellation；
6. 接入 TaskListener、Group listener、GlobalPar retain 与 purger；
7. 接入 TaskGraph 的 outer/member 真实边并验证无 membership 边；
8. 完成全部竞态测试、文档和迁移说明；
9. 运行 `mvn spotless:apply` 与 `mvn test`。

## 16. 完成定义

只有同时满足以下条件才算实现完成：

- 公共 API、状态机和结果原因符合本文；
- 没有新增 current-group ThreadLocal/TTL 或虚构 Group Batch；
- Group 与 Batch 共享单任务运行内核，没有复制取消/phase/ScopedCallable 逻辑；
- build admission、cancel、run、rejection、timeout 的竞态均有确定且测试覆盖的结果；
- 所有冻结 public future 必然终态；
- TaskGraph 只记录真实结构化依赖；
- 全量 Maven 测试通过且 Java 8 main source 兼容；
- 用户文档说明 spec/submit、`TaskRef`、close、cancel、deadline、成员取消和 observation 生命周期。
