# TaskGroup 设计契约：API 与选项

> 本系列是 `TaskGroup` 的独立实施规范（由原《独立并行任务组最终设计契约》按章节拆分）。
> 实现者只依赖本系列和当前代码库即可完成开发，不需要再参考早期草稿。文中的 MUST、
> MUST NOT、SHOULD 分别表示必须、禁止和推荐。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。
> 面向使用者的 API 说明见 [使用指南](../../docs/zh/user-guide.md) 与 [v0.2 迁移指南](../../docs/zh/migration-v0.2.md)。

## 1. 目标与非目标

`TaskGroup` 用于在一个显式协调范围内提交少量、具名、类型可以不同、彼此没有数据依赖的任务：

```text
account-page group
├─ get-user       -> User
├─ get-orders     -> List<Order>
└─ get-inventory  -> Inventory
```

它提供：

- 通过 `TaskGroupDefinition` 收集具名任务定义，并在唯一的 submit 时点统一冻结和提交；
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
| 集合形成 | `map()` 调用时固定 | `TaskGroupDefinition.Builder.task()` 配置，`TaskGroup.submit()` 冻结并统一提交 |
| 成员身份 | `taskIndex` | 唯一 `memberName` |
| 返回类型 | 全部为同一个 `R` | 每个成员可以有不同 `T` |
| executor | 整个 Batch 使用一个 `Par` | 每个成员选择自己的 `Par` |
| 调度 | `SlidingWindowSubmitter` 滑动窗口 | submit 时为全部成员准备后逐一独立提交 |
| 完成 | 固定 futures 全部终态 | submit 时冻结的全部成员 future 终态 |
| 关系 | 嵌套 Batch 可以形成 TaskGraph 依赖边 | membership 本身不是依赖边 |

Group MUST NOT 通过 `Par.map(singletonList, ...)` 实现，也 MUST NOT 对外暴露 `TaskBatchResult<Object>`。

## 3. 公共 API

公共类型放在 `io.github.monadrome.parallelinscope.scope`；监听 SPI 放在 `io.github.monadrome.parallelinscope.spi`。

### 3.1 创建与使用

组由不可变、可复用的 `TaskGroupDefinition` 描述，经一次性
`TaskGroup.submit(global, definition)` 冻结并统一提交：

```java
TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(
        MultiTaskOptions.of("account-page")
                .timeout(Duration.ofSeconds(3))
                .build());

TaskKey<User> user = definition.task(
            new TaskKey<User>("get-user") {},
            "user", userService::getUser,
            MultiTaskOptions.builder().inheritTimeout().build());
TaskKey<List<Order>> orders = definition.task(
            new TaskKey<List<Order>>("get-orders") {},
            "order", orderService::getOrders,
            MultiTaskOptions.builder().inheritTimeout().build());
TaskKey<Inventory> inventory = definition.task(
            new TaskKey<Inventory>("get-inventory") {},
            "inventory", inventoryService::getInventory,
            MultiTaskOptions.builder().inheritTimeout().build());

try (TaskGroup group = TaskGroup.submit(global, definition.build())) {
    User userValue = group.future(user).get();
    TaskGroupResult result = group.completionFuture().get();
}
```

`TaskGroupDefinition` 是纯数据描述；`TaskGroup` 是 submit 后的运行对象。当前公共面：

```java
public final class TaskGroupDefinition {
    public static Builder builder(MultiTaskOptions groupOptions);
    public MultiTaskOptions groupOptions();
    public List<TaskDefinition<?>> tasks();

    public static final class Builder {
        public <T> TaskKey<T> task(
                TaskKey<T> key,
                String executorName,
                Callable<T> callable,
                MultiTaskOptions options);
        public TaskGroupDefinition build();
    }
}

public abstract class TaskKey<T> {
    protected TaskKey(String name);
    public final String name();
    public final TypeToken<T> resultType();
}

public final class TaskGroup implements AutoCloseable {
    public static TaskGroup submit(GlobalPar env, TaskGroupDefinition definition);

    public String groupId();
    public String groupName();
    public void cancel();
    public ListenableFuture<TaskGroupResult> completionFuture();

    public Optional<ListenableFuture<?>> findMember(String memberName);
    public Map<String, ListenableFuture<?>> members();
    public <T> ListenableFuture<T> future(TaskKey<T> key);

    @Override
    public void close();
}
```

语义：

- `TaskGroupDefinition.Builder.task()` 只校验并保存不可变任务定义（key 为 null、name 重复、参数为
  null 立即拒绝；name 的 null/空白校验由 `TaskKey` 构造器完成）；不得提交 executor、启动
  timer、创建 `MultiTaskContext`/`TaskExecutionContext` 或占用运行期资源；
- `TaskGroup.submit()` 是唯一的冻结与提交入口；它按提交线程解析结构父任务与
  observation、创建并注册全部成员后才允许任何成员进入 executor；definition 本身可重复提交；
- `TaskKey<T>` 由调用方以匿名子类创建（`new TaskKey<List<Order>>("orders") {}`），在运行时
  捕获结果类型，不携带执行状态；`group.future(key)` 在组内解析成员
  future，名称不属于该组、或 key 的 raw 结果类型不能覆盖注册类型时抛
  `IllegalArgumentException`；
- `cancel()` 幂等、非阻塞，固定 `CANCELED`（若尚未固定）并取消未完成成员；
- `close()` 是异常安全清理：若仍有未完成成员，语义等同 `cancel()`；若所有成员已经终态或空组则无副作用；
- `completionFuture()` 在全部冻结成员的公开 future 终态后完成，不存在另行封口条件；
- `members()` 返回按定义顺序排列、不可修改的完整集合；
- `findMember()`/`members()` 在 Group 返回给调用方时即可看见全部冻结成员。

`TaskKey<T>` 是调用方创建的类型化键，通过匿名子类在运行时捕获结果类型，不携带执行状态；
键按 name 值相等，因此声明父类型的键与注册键是同一个键。异构任务的 future 在统一
submit 时创建，调用方用配置期注册的键在提交后取回类型安全的 future。definition 不捕获线程
上下文，因此结构归属始终由提交现场决定。

### 3.2 组级与成员级选项

组级与成员级选项统一为 `MultiTaskOptions`；组读取 name/timeout/listeners，成员与 combine 的
执行行为使用 timeout/taskType/rejectEnqueue。它们的身份来自 `TaskKey.name()`，不读取 options
的 name/listeners；成员是单任务，`parallelism` 虽被解析但不影响执行，成员内部嵌套提交读取
嵌套提交自己的 options：

```java
public final class MultiTaskOptions {
    public static Builder builder();
    public static Builder of(String name);

    public String name();
    public int parallelism();
    public Optional<Duration> timeout();
    public TaskType taskType();
    public boolean rejectEnqueue();
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
public enum TaskOutcome {
    RUNNING,
    SUCCESS,
    USER_FAILURE,
    SUBMISSION_FAILURE,
    MEMBER_CANCELED,
    GROUP_CANCELED,
    FAIL_FAST,
    TIMEOUT
}
```

`TaskOutcome` 是全库统一的单任务终态词汇，同时服务批量报告、组成员结果与组级结果；`RUNNING`
表示尚未终态，不会出现在完成后的结果快照中。组级只会出现 `SUCCESS`、`USER_FAILURE`、
`SUBMISSION_FAILURE`、`TIMEOUT`、`MEMBER_CANCELED`、`GROUP_CANCELED`：fail-fast 时组沿用失败
成员自己的 outcome（`USER_FAILURE`/`SUBMISSION_FAILURE`），`MEMBER_CANCELED` 表示取消源自
组员或直接作用于组员，`GROUP_CANCELED` 表示组被整体取消或取消自上传播。

`TaskGroupResult` 和成员结果必须是完成后的不可变快照：

```java
public final class TaskGroupResult {
    public String groupId();
    public String groupName();
    public long startTimeNanos();
    public long endTimeNanos();
    public long deadlineNanos();
    public TaskOutcome outcome();
    public @Nullable String failedTaskName();
    public Map<String, TaskCompletion<?>> members();
    public int memberCount();
}
```

成员快照与成员级监听器事件统一为同一个已完成任务记录 `TaskCompletion`（batch 元素与
group 成员共用），字段为 `taskName()`/`unitId()`/`taskIndex()`/三个时间戳/`outcome()`/
`result()`/`failure()`，派生 `successful()`/`enqueued()`/三个 Duration。其中 `result()`
只在监听器投递成功任务时非 null（组成员结果留在其 future），`taskIndex()` 对组成员恒为 0；
组快照和监听器事件的 `taskName()` 均取注册 key 的 name。

要求：

- Map 按任务定义顺序稳定输出且不可修改；
- `failure` 仅用于 `USER_FAILURE` 和 `SUBMISSION_FAILURE`；
- 结果保存完成原因，MUST NOT 仅根据 `Future.isCancelled()` 反推原因；
- 成员结果只携带打平后的只读数据，不暴露 `MultiTaskContext` 等引擎管道；运行期的 `TaskExecutionContext` 在完成快照之后 MUST NOT 再被安装为 current task；
- `completionFuture()` 正常完成并返回 `TaskGroupResult`，组的非 `SUCCESS` outcome 是结果数据，不通过 completion future 本身抛错表达；
- 单个成员 future 保持普通 Guava 语义：成功返回值、失败抛 `ExecutionException`、取消表现为 cancelled。
