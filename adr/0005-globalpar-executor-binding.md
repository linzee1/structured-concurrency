# ADR 0005: GlobalPar 执行器绑定与全局管理

- Status: Accepted
- Date: 2026-08-22
- Decision scope: GlobalPar executor topology, per-batch context, and cross-resource observation
- Supersedes: None

## Context

执行器选择、跨线程取消、批次 deadline 和 deadlock/purge 观测原先分散在旧配置与字符串绑定
入口中，导致公共 API 同时暴露多套模型。项目需要在 Java 8 字节码约束下支持多个逻辑执行
入口、稳定的 supplied-executor identity，以及可验证的嵌套批次语义。

## Decision

采用不可变 `GlobalPar` 拓扑：每个 `Par` 在构建时绑定一个 supplied executor；单次调用使用
`BatchExecutionOptions`，解析为只属于该批次的 `BatchExecutionContext`。`ExecutorRuntime` 仅是包内
技术绑定；取消沿显式 parent context 向下传播；跨 Par 的任务图和 purge 观测由
`GlobalPar`/observation scope 持有。旧 `ParConfig`、`ParOptions`、`ExecutorBinding` 入口不保留。

## Alternatives Considered

- 在每次 `map` 调用传入 executor 或 executor name。拒绝，因为会让资源选择和业务调用耦合，
  也无法保证 identity、策略和生命周期的一致性。
- 保留旧配置对象作为兼容层并让新模型并存。拒绝，因为两套默认值、listener 和上下文来源
  会重新产生漂移。
- 以 executor 名称作为任务图节点。拒绝，因为不同批次的同名任务会错误合并并制造假 cycle。

## Consequences

业务调用更简单，批次取消/deadline 和跨 executor 观测有单一事实来源；拓扑变更必须重新构建
`GlobalPar`，旧 API 用户需要一次 breaking migration。borrowed supplied executor 仍由创建者
负责关闭，框架只关闭自身 timer、purger 和提交器资源。

## 1. 结论

执行器不应由每次 `map` 调用选择，也不应保存在 `ParConfig` 的字符串注册表中。

理想模型是：

```text
GlobalExecutionPolicy = GlobalPar 的全局执行策略
Par         = GlobalPar 中一个已绑定执行器的逻辑入口
GlobalPar = 应用级、不可变的多个 Par 容器 + 跨 Par/线程池策略
```

更完整的生命周期模型是：

```text
GlobalParDefinition / GlobalPar
    └─ 应用生命周期：启动时构建，应用关闭时释放框架运行时
         ├─ GlobalParPolicy
         │    ├─ deadlock policy
         │    └─ purge policy
         ├─ Par (多个逻辑执行入口)
         └─ ExecutorRuntime (按 supplied executor identity)

Par
    └─ ParDefinition + ExecutorRuntime 引用

BatchExecutionOptions
    └─ 一次 map 调用的不可变用户输入

BatchExecutionContext
    └─ 一次 map 调用的不可变运行时快照
         ├─ normalized parallelism / timeout
         ├─ CancellationToken
         ├─ TaskGraphObservationContext
         └─ batch metadata

TaskExecutionContext
    └─ 单个元素任务的短生命周期上下文
```

一个 `Par` 从创建开始就稳定代表一个执行资源：

```text
GlobalPar
    ├─ defaultPar ──▶ Par(default, defaultPool)
    ├─ database   ──▶ Par(database, databasePool)
    ├─ http       ──▶ Par(http, httpPool)
    └─ cpu        ──▶ Par(cpu, cpuPool)
```

应用可以在启动阶段向 `GlobalPar.Builder` 注册一次多个 `Par`。构建完成后 `GlobalPar` 不可变。业务组件从 `GlobalPar` 取得一次所需的 `Par`，随后只调用：

```java
databasePar.map(ids, repository::load, options);
```

业务调用不再传 executor、executor name 或另一层 `ExecutionTarget`。

## 2. 第一性原理

执行器相关设计实际需要回答十个独立问题：

| 问题 | 最佳归属 |
|---|---|
| 任务提交到哪个资源 | `Par` |
| 如何获得 `ListenableFuture` | `Par` 内部运行时 |
| 全局默认超时和执行参数是什么 | `GlobalExecutionPolicy` |
| 跨多个 `Par` 的 deadlock 检测和线程池 purge 策略是什么 | `GlobalPar` |
| 一次批量任务的显式设置是什么 | `BatchExecutionOptions` |
| 一次批量任务解析后的运行状态是什么 | `BatchExecutionContext` |
| 单个元素任务如何访问取消和诊断上下文 | `TaskExecutionContext` |
| 如何识别同一个真实池 | supplied executor identity |
| 应用如何集中管理多个执行入口 | `GlobalPar` |
| 日志如何显示业务含义 | `Par.displayName` |

问题的根源是把这些职责都压进了：

```text
String executorName -> ExecutorBinding
```

名字同时承担了查找键、资源身份和诊断标签，但三者不是同一件事：

```text
查找键：应用组装时找到哪个 Par
资源身份：用户传入 ExecutorService 的对象 identity
诊断标签：日志中展示哪个业务名称
```

理想设计必须把它们分开。

## 3. 设计目标

1. 一个 `Par` 绑定且只绑定一个 supplied `ExecutorService`。
2. `Par.map` 不接收 executor 或 executor name。
3. `GlobalExecutionPolicy` 只描述应用级默认执行策略，不保存执行器注册表。
4. `GlobalPar` 可以集中注册一次多个 `Par`，构建后不可变。
5. 字符串名称只在 `GlobalPar` 的应用组装边界出现。
6. 业务组件依赖 `Par` 对象，不依赖 `GlobalPar` service locator。
7. 明确区分用户传入的 `ExecutorService` 和框架提交使用的 `ListeningExecutorService`。
8. 真实身份、deadlock 探测和 purge 必须基于 supplied executor，而不是包装对象。
9. 所有 `Par` 使用同一个 `GlobalExecutionPolicy`；跨 `Par` 的 deadlock/purge 配置只由 `GlobalPar` 管理。
10. `TaskType` 描述任务行为，不决定选择哪个 `Par`。
11. Java 8 是基础运行时；Java 21 虚拟线程通过同一 `ExecutorService` 抽象接入。

## 4. 明确不做

- 不保留 `Par.map(String executorName, ...)`。
- 不提供 `Par.map(ExecutorService, ...)`。
- 不引入公共 `ExecutionTarget`。
- 不让 `GlobalExecutionPolicy` 注册或查找 executor。
- 不允许运行期向 `GlobalPar` 增删或替换 `Par`。
- 不默认使用 `ForkJoinPool.commonPool()`。
- 不根据父任务上下文自动选择另一个 `Par`。
- 不让 `TaskType` 隐式映射到某个池。
- 不让 `GlobalPar` 负责关闭用户创建的 executor。

## 5. 核心类型

### 5.1 GlobalExecutionPolicy：应用级默认策略

不再引入独立的 `ParDefaults`。默认值属于 `GlobalPar`，因为一次请求可能跨越多个 `Par`，必须由同一个应用执行域解析。

```text
GlobalExecutionPolicy
    ├─ default timeout
    ├─ timer service
    ├─ task listeners
    ├─ rejection/queue defaults
    └─ optional per-Par policy overrides
```

建议删除：

```java
Builder.executor(String, ExecutorService)
getExecutor(String)
getExecutorBinding(String)
```

`GlobalExecutionPolicy` 是构建后不可变的声明对象，不保存 executor runtime，不拥有任务图，也不保存线程池 purge 状态。它只回答：当某个 `Par` 的一次调用没有显式指定值时，应用默认采用什么局部参数。

所有注册到同一个 `GlobalPar` 的 `Par` 共享同一个全局策略：

```java
GlobalExecutionPolicy policy = GlobalExecutionPolicy.builder()
        .taskListener(listener)
        .build();

GlobalPar global = GlobalPar.builder()
        .executionPolicy(policy)
        .register("database", databasePool)
        .register("http", httpPool)
        .build();
```

如果某个执行入口需要不同的默认 timeout 或 listener，使用 `GlobalExecutionPolicy` 的显式 Par override。不要通过给 `Par` 附加一个新的 defaults 对象来形成第二个配置作用域。

### 5.2 Par：逻辑执行入口

`Par` 是执行门面，也是用户注入业务组件的资源句柄。

建议的公共 API：

```java
public final class Par {

    public <T, R> AsyncBatchResult<R> map(
            @Nullable List<T> list,
            Function<? super T, ? extends R> function,
            BatchExecutionOptions options);

    public GlobalPar getGlobalPar();

    public String getDisplayName();
}
```

`Par` 创建后不可变：

```text
Par
    ├─ GlobalPar owner
    └─ ExecutorRuntime runtime
```

不提供：

```java
register(...)
replaceExecutor(...)
map(executor, ...)
map(executorName, ...)
```

选择执行器是构造 `Par` 的职责，不是执行一次批次的职责。

`bind` 不属于本设计的核心 `Par` API：它接收已经由任意执行器提交的 futures，无法证明这些 futures 属于当前 `Par`，也无法可靠补齐 executor identity。未来如需观察外部 futures，应单独设计 `FutureObservation`/`FutureBinder` API，并要求调用方显式提供来源 identity；不得把它伪装成当前 `Par` 的执行操作。

`Par` 是逻辑入口，不是线程池本身，也不是一次批量任务。它只持有所属 `GlobalPar` 和 `ExecutorRuntime` 引用；它不持有某次调用的 list、timeout deadline 或 cancellation token。理想情况下，`Par` 只能由 `GlobalPar.Builder` 创建，避免一个 `Par` 被错误注册到多个 `GlobalPar`。

### 5.3 BatchExecutionOptions：一次调用的用户输入

`BatchExecutionOptions` 只表示调用方对一次 `map` 的显式要求：

```text
BatchExecutionOptions
    ├─ taskName                 // 诊断标签
    ├─ requestedParallelism    // 本批次并发上限
    ├─ timeout                 // 本批次 deadline 请求
    ├─ taskType                // CPU_BOUND / IO_BOUND
    └─ rejectEnqueue           // 本批次队列策略提示
```

它必须满足：

- 不包含 executor、Par、GlobalPar、listener、purger 或 runtime state。
- 不包含 `CancellationToken`、future 列表、实际 task count 或线程池能力。
- 用户传入对象保持不可变；默认值合并和 parallelism 截断不回写原对象。
- `TaskType` 只描述任务行为，不负责选择执行资源。

### 5.4 BatchExecutionContext：一次批量任务的运行时上下文

`Par.map` 收到 `BatchExecutionOptions` 后创建一个短生命周期的 `BatchExecutionContext`，把声明输入解析成可执行计划：

```text
BatchExecutionContext
    ├─ Par / ExecutorIdentity
    ├─ taskName
    ├─ taskCount
    ├─ effectiveParallelism
    ├─ effectiveTimeout / deadline
    ├─ effective TaskType / queue policy
    ├─ CancellationToken
    ├─ TaskGraphObservationContext
    └─ AsyncBatchResult
```

这是解决当前 `ParOptions.formalized(...)` 职责混杂的关键：`BatchExecutionOptions` 是输入，`BatchExecutionContext` 是本次运行状态。批次结束后，context 可释放；不能把它缓存回 `Par` 或 `GlobalPar`。

### 5.5 CancellationToken 与嵌套批次

取消传播必须基于显式的父子 `BatchExecutionContext` 链，而不是依赖 executor 或线程本地变量：

```text
outer BatchExecutionContext
    └─ outer CancellationToken
         └─ inner BatchExecutionContext
              └─ inner CancellationToken
```

嵌套调用规则：

- `innerPar.map(...)` 在父任务中被直接调用时，子 context 的 parent token 取当前 `TaskExecutionContext` 的 token。
- 子批次可以使用完全不同的 `ExecutorRuntime`；取消链不因跨线程池而断开。
- 取消只默认向下传播：父批次取消子批次；子批次失败是否导致父任务失败，由父 callable 显式决定。
- 子 deadline 必须受父 deadline 限制：

```text
childDeadline = min(parentDeadline, now + childRequestedTimeout)
```

- `CancellationToken` 负责传播取消信号和取消 futures；`BatchExecutionContext` 负责保存 deadline、timer 和观测上下文。

`TaskScopeTl`/`ThreadRelay` 只能作为异步边界的上下文传输机制，不能作为取消链的唯一来源。未被 TTL 包装的第三方 executor、future callback 或自定义异步 API 可能无法可靠传播 ThreadLocal，因此框架应在提交边界显式捕获并安装 `TaskExecutionContext`。

### 5.6 TaskExecutionContext：单个元素任务上下文

每个元素任务只需要短生命周期上下文：

```text
TaskExecutionContext
    ├─ batchId / taskName
    ├─ CancellationToken
    ├─ Par label
    ├─ supplied ExecutorIdentity
    ├─ resolved execution options
    └─ parent observation context
```

它通过 `TaskScopeTl`/`ThreadRelay` 传播给当前 callable 和嵌套调用。任务完成后必须清理；不得把完整 `Par`、`GlobalPar` 或 pool runtime 放进线程本地上下文。

### 5.7 ExecutorRuntime：内部技术绑定

`ExecutionTarget` 和 `ExecutorBinding` 功能重叠，因此都不作为公共概念存在。框架内部保留一个 package-private 的技术对象：

```java
final class ExecutorRuntime {
    private final ExecutorService suppliedExecutor;
    private final ListeningExecutorService submissionExecutor;
    private final boolean submissionExecutorIsAdapter;
    private final ExecutorIdentity executorIdentity;
    private final String displayName;
    private final BlockingRisk blockingRisk;
    private final Consumer<? super ExecutionPhase> phaseObserver;
}
```

`ExecutorIdentity` 必须是独立的不可变值对象，而不是 identity hash 字符串：

```java
final class ExecutorIdentity {
    private final ExecutorService supplied;

    // equals/hashCode use reference equality of supplied.
    // toString is class name + identityHashCode for diagnostics only.
}
```

对象引用是唯一的资源判定依据；`identityHashCode` 只用于日志，不能作为图节点 key。identity 对象由运行时持有强引用，直到对应 `ExecutorRuntime` 释放。

它在 `Par` 创建时构造一次，此后所有 `map` 调用直接复用。

字段职责：

| 字段 | 含义 |
|---|---|
| `suppliedExecutor` | 用户传入的精确对象 |
| `submissionExecutor` | 框架实际提交任务的 Guava 执行器 |
| `submissionExecutorIsAdapter` | 是否由框架创建 listening adapter |
| `executorIdentity` | supplied executor 的 identity |
| `displayName` | 人类可读的诊断名称 |
| `blockingRisk` | 嵌套阻塞的保守风险能力 |
| `phaseObserver` | 取消队列任务时通知 purger |

### 5.4 GlobalPar：应用组合作用域

`GlobalPar` 是不可变的 `Par` 容器：

```java
public final class GlobalPar {

    public static Builder builder();

    public Par defaultPar();

    public Par par(String name);

    public Optional<Par> find(String name);

    public GlobalParDeadlockPolicy deadlockPolicy();

    public GlobalParPurgePolicy purgePolicy();

    public static void installGlobal(GlobalPar globalPar);

    public static GlobalPar global();
}
```

构建方式：

```java
GlobalPar globalPar = GlobalPar.builder()
        .executionPolicy(policy)
        .defaultPar("database")
        .register("database", databasePool)
        .register("http", httpPool)
        .register("cpu", cpuPool)
        .build();
```

Builder 规则：

- 名称非空。
- 名称重复立即失败，不允许后注册覆盖前注册。
- `defaultPar` 可选，但最多一个；理想 builder 以名称指定 default。
- Builder 创建并拥有注册的 `Par`，不接受已属于其他 `GlobalPar` 的 `Par`。
- build 后复制为不可变 map。
- `deadlockPolicy` 和 `purgePolicy` 在 Builder 中配置，build 后不可变。
- `GlobalPar` 为每个已注册 supplied executor 建立一个线程池级运行时 coordinator。
- `GlobalPar` 不负责 executor shutdown。

`register` 是构建期一次性组装操作，不是运行期可变注册。

`GlobalPar` 的两个跨资源策略对象：

```text
GlobalPar
    ├─ immutable Map<String, Par>
    ├─ GlobalParDeadlockPolicy
    │    ├─ enabled
    │    ├─ listeners
    │    └─ graph retention/reporting rules
    └─ GlobalParPurgePolicy
         ├─ enabled
         ├─ queue pressure threshold
         ├─ cancelled-task ratio threshold
         └─ Map<ExecutorIdentity, ExecutorRuntime>
```

`GlobalParDeadlockPolicy` 面向整个 `GlobalPar` 看到的任务图；`GlobalParPurgePolicy` 面向其中注册的物理线程池。它们不是某个 `Par` 的局部属性，也不随单次 `map` 调用变化。

### 5.7 生命周期与状态归属

```text
应用启动
  └─ build GlobalPar
       ├─ 校验并冻结 Par 注册表
       ├─ 创建 GlobalParDeadlockPolicy
       ├─ 为每个 supplied executor 创建/复用 ExecutorRuntime
       └─ 注入 Par 到业务组件

一次 map 调用
  └─ Par.map(BatchExecutionOptions)
       ├─ 合并 GlobalExecutionPolicy + BatchExecutionOptions
       ├─ 创建 BatchExecutionContext
       ├─ 注册到 TaskGraphObservationContext
       ├─ 创建 N 个 TaskExecutionContext
       └─ 返回 AsyncBatchResult

请求结束
  └─ TaskGraphObservationContext.close()
       └─ GlobalParDeadlockPolicy 执行一次全图检测

应用关闭
  └─ GlobalPar.close()
       ├─ 停止 GlobalPar 自己创建的 timer/coordinator
       └─ 不关闭 borrowed supplied executor
```

生命周期原则：声明对象可以长期复用；运行时对象必须绑定明确的 owner；一次批次状态不能泄漏到 `Par` 或 `GlobalPar`；请求级观测必须显式结束，不能依赖某个 `Par` 的最后一次 `map` 调用来猜测请求结束。

## 6. supplied 与 submission executor

### 6.1 两个对象的边界

用户传入的执行器和框架提交任务的执行器可能不是同一个 Java 对象：

```text
ExecutorService suppliedExecutor
        │
        ├─ instanceof ListeningExecutorService
        │       └─ submissionExecutor = suppliedExecutor
        │          isAdapter = false
        │
        └─ ordinary ExecutorService
                └─ submissionExecutor =
                       MoreExecutors.listeningDecorator(suppliedExecutor)
                   isAdapter = true
```

必须遵守以下规则：

| 行为 | 使用哪个对象 |
|---|---|
| 实际 `submit` | submission executor |
| 资源 identity | supplied executor |
| `ThreadPoolExecutor` 类型判断 | supplied executor |
| 队列检查 | supplied executor |
| deadlock/blocking risk 推导 | supplied executor |
| `HeuristicPurger` 状态键 | supplied executor |
| shutdown 所有权 | supplied executor 的创建者 |
| 日志类名和 identity | supplied executor |

框架不得把 listening adapter 当成真实线程池。

`PurgeCoordinatorRegistry` 是 `GlobalParPurgePolicy` 的运行时部分，不属于 `GlobalExecutionPolicy`：

- key 是 `ExecutorIdentity`，value 是一个物理池级 `PoolState`/coordinator。
- 同一 supplied executor 在多个 `Par` 中只注册一次；一个 `GlobalPar` 内的所有 `Par` 共享该线程池 coordinator。
- purge enablement/threshold 由 `GlobalParPurgePolicy` 统一决定；coordinator 只负责按线程池状态合并触发和执行 purge。
- 如果同一个 executor 被放进两个独立 `GlobalPar`，两套应用组装作用域各自拥有 policy/coordinator；这不代表两个独立 registry 的资源 identity 被错误合并。
- 不透明 wrapper 无法探测底层队列时，不注册队列状态，observer 为 no-op。
- registry 不以 `submissionExecutor` 或 adapter identity 为 key。

### 6.2 用户传入已有包装器

如果用户传入一个 `ListeningExecutorService`，该对象本身就是 supplied executor：

```text
suppliedExecutor == submissionExecutor
```

框架不应反射解包第三方 decorator，因为无法建立通用、可靠的底层资源协议。

例如：

```text
ThreadPoolExecutor rawPool
    ├─ decoratorA(rawPool)
    └─ decoratorB(rawPool)
```

若用户把 A 和 B 分别传给两个 `Par`，框架只能保守地把它们视为两个 supplied identity。希望共享身份时，应用必须复用同一个 supplied executor 对象或同一个 `Par`。

传入不透明包装器还会降低能力探测精度：框架可能无法看到底层 `ThreadPoolExecutor` 的队列、最大线程数和 purge 能力。此时 `BlockingRisk` 必须取保守值，purge observer 必须退化为 no-op。需要完整诊断和队列维护能力时，推荐用户传入原始 `ExecutorService`，由框架创建 listening adapter。

### 6.3 生命周期

默认所有 `Par` 都是 borrowed：

```text
谁创建 ExecutorService，谁负责 shutdown
```

`Par`、`GlobalPar` 和 listening adapter 都不能隐式关闭 supplied executor。

如果未来确实需要托管资源，必须设计独立、显式的所有权 API；不在本次设计中加入。

## 7. GlobalPar 的价值与边界

### 7.1 为什么全局管理是合理的

应用通常只有一套稳定的执行资源拓扑。集中定义多个 `Par` 有明确价值：

- 线程池及其业务用途集中可见。
- `GlobalExecutionPolicy` 的全局与 Par override 关系集中可见。
- default `Par` 明确。
- 启动时一次性校验名字。
- 配置文件和依赖注入框架容易接入。
- 业务代码不必重复构造 `Par`。

因此，全局本身不是问题。真正危险的是：

```text
全局 + 可变 + 业务调用期字符串查找
```

推荐的是：

```text
全局 + 构建期注册 + 构建后不可变 + 业务组件注入 Par
```

### 7.2 与其他方案比较

| 方案 | 调用体验 | 隔离 | 类型安全 | 生命周期清晰度 | 判断 |
|---|---:|---:|---:|---:|---|
| 每次传 executor | 中 | 强 | 中 | 中 | 重复选择资源 |
| `ParConfig` 字符串注册表 | 高 | 中 | 弱 | 中 | 配置与资源混合 |
| 公共 `ExecutionTarget` | 高 | 强 | 强 | 中 | 与 Par/内部 binding 重叠 |
| 一个 `Par` 绑定一个池 | 很高 | 强 | 强 | 强 | 核心模型 |
| 不可变 `GlobalPar` 管多个 `Par` | 很高 | 中/强 | 组装后强 | 强 | 推荐组合 |
| 全局可变注册表 | 表面很高 | 弱 | 弱 | 弱 | 不采用 |

### 7.3 防止退化为 service locator

不推荐：

```java
class OrderService {
    void load() {
        GlobalPar.global()
                .par("database")
                .map(ids, repository::load, options);
    }
}
```

推荐：

```java
class OrderService {
    private final Par databasePar;

    OrderService(Par databasePar) {
        this.databasePar = databasePar;
    }

    void load() {
        databasePar.map(ids, repository::load, options);
    }
}
```

`GlobalPar` 只出现在 composition root。字符串查找只发生一次。

### 7.4 可选静态全局入口

```java
GlobalPar.installGlobal(globalPar);
GlobalPar runtime = GlobalPar.global();
```

约束：

- 只能成功安装一次。
- 读取前未安装应明确失败。
- 不支持 reset、replace 或 clear。
- 测试和嵌入式场景优先显式创建 `GlobalPar`，不使用静态入口。

静态入口是便利能力，不是核心依赖机制。

## 8. 上下文传播与任务图

### 8.1 传播内容

`ThreadRelay` 不再只传播 executor name，而是传播：

```java
enum RelayItem {
    CANCELLATION_TOKEN,
    PARALLEL_OPTIONS,
    TASK_NAME,
    EXECUTOR_IDENTITY,
    PAR_LABEL
}
```

- `EXECUTOR_IDENTITY` 用于父子执行资源关系判断。
- `PAR_LABEL` 只用于日志和诊断。
- 不传播完整 `Par`、`GlobalPar` 或 `ExecutorRuntime`。

### 8.2 嵌套调用

嵌套调用使用代码中显式引用的 `Par`：

```java
outerPar.map(groups, group ->
        innerPar.map(group.items(), this::transform, innerOptions),
        outerOptions);
```

规则：

- 调用哪个 `Par`，就使用哪个 `Par` 绑定的 executor。
- 不自动继承父 `Par`。
- 不根据 `TaskType` 自动切换 `Par`。
- 父 executor identity 仅用于取消链和任务图诊断。

如果 `outerPar` 和 `innerPar` 绑定同一个有限池，任务图必须按 supplied identity 识别为自环，而不是按两个 Par 名称误判成两个池。

### 8.3 任务图

`TaskEdge` 建议携带：

```java
TaskEdge {
    ExecutorIdentity sourceExecutor;
    ExecutorIdentity targetExecutor;
    String sourceParLabel;
    String targetParLabel;
    int parallelism;
    TaskType taskType;
    BlockingRisk blockingRisk;
}
```

图节点相等只看 `ExecutorIdentity` 的引用相等语义：

```text
same supplied object -> 同一节点
different supplied object -> 不同节点
```

Par label 只用于输出：

```text
outer(database@1a2b) -> inner(database@1a2b)
```

### 8.4 GlobalPar 级请求观测

任务图可能跨越 `databasePar`、`httpPar` 和 `cpuPar`，因此 deadlock 检测不能由任意一个 `ParConfig` 或单个 `Par` 决定。执行上下文绑定一个 `TaskGraphObservationContext`，所有注册 `Par` 的边都写入同一张图。

请求结束时：

1. 由 `GlobalParDeadlockPolicy` 决定是否构建和检查完整任务图。
2. 由 GlobalPar 统一执行 task graph cycle、executor cycle 和 self-loop 检测。
3. listener 集合来自 `GlobalParDeadlockPolicy`，按 listener identity 去重。
4. `TaskGraph.finishObservation(TaskGraphObservationContext)` 是生命周期 API；不再接收 `ParConfig`。

如果一个请求需要使用两个独立的 `GlobalPar`，必须显式创建两个 observation context；它们的任务图和 listener 不自动合并。这样“跨多个 Par 追踪”有一个明确的所有权边界：同一个 GlobalPar 内统一追踪，不同 GlobalPar 之间保持隔离。

## 9. TaskType 与 Par 选择

`TaskType` 继续描述任务行为：

- `CPU_BOUND`：计算密集型。
- `IO_BOUND`：允许阻塞等待。

它可以影响：

- `SmartBlockingQueue.offer()`。
- 拒绝后的执行策略。
- 默认 parallelism。
- 监控分类。

它不能隐式决定：

```text
IO_BOUND  -> httpPar
CPU_BOUND -> cpuPar
```

数据库和 HTTP 都可能是 IO，却需要不同隔离池。执行资源由 `Par` 表达，任务行为由 `BatchExecutionOptions` 表达。

若未来确实需要动态路由，应设计显式 `ParResolver`：

```java
interface ParResolver {
    Par resolve(BatchExecutionOptions options, ExecutionContext context);
}
```

它属于应用路由层，不属于 `ParConfig`，也不是 `TaskType` 的隐藏副作用。

## 10. Java 8 与 Java 21

### 10.1 Java 8

公共 API 只依赖 `ExecutorService`。Guava `ListeningExecutorService` 是内部提交能力，不要求普通用户预先包装。

### 10.2 Java 21 虚拟线程

虚拟线程只是另一种 supplied executor。Java 8 主源码不能直接引用 Java 21 API，因此能力判断必须通过可选 SPI 注入，而不是按类名反射猜测：

```java
interface ExecutorCapabilityProvider {
    BlockingRisk blockingRisk(ExecutorService suppliedExecutor);
}
```

Java 8 默认 provider 只识别可靠的 `ThreadPoolExecutor` 能力，无法识别的 executor 返回 `UNKNOWN` 保守值。Java 21 集成模块可以额外注册 provider，并在 Java 21-only 文档/示例中创建虚拟线程 executor：

```java
GlobalPar virtualGlobal = GlobalPar.builder()
        .register("virtual-io", Executors.newVirtualThreadPerTaskExecutor())
        .defaultPar("virtual-io")
        .build();
Par virtualIoPar = virtualGlobal.defaultPar();
```

框架不能假设所有 executor 都是 `ThreadPoolExecutor`：

- 非 `ThreadPoolExecutor` 不接入队列 purger。
- virtual-thread-per-task executor 由 Java 21 provider 返回专门的 `BlockingRisk`。
- `BatchExecutionOptions.parallelism` 仍然提供业务级限流。
- 取消、超时、TTL 和任务图行为保持一致。

Java 8 编译门槛只覆盖公共 API 和核心实现；虚拟线程示例与 provider 放在独立的 Java 21 profile/module 中，不能出现在 Java 8 源码编译路径。

## 11. 错误模型

建议异常：

```java
final class InvalidParBindingException extends IllegalArgumentException
final class MissingParException extends IllegalStateException
```

错误场景：

| 场景 | 行为 |
|---|---|
| 创建 `Par` 时 config/executor 为空 | 立即失败 |
| `GlobalPar` 名称为空或重复 | build 阶段失败 |
| 查找不存在的命名 `Par` | `MissingParException` |
| 未配置 default 却调用 `defaultPar()` | `MissingParException` |
| supplied executor 已 shutdown | 保留 `RejectedExecutionException` 并附加 Par 诊断信息 |
| 重复安装静态 global | `IllegalStateException` |

诊断错误至少包含：

```text
par displayName + executor class + identity hash + task name
```

## 12. 观测字段

每次提交应关联：

```text
taskName
parLabel
executorClass
executorIdentity
taskType
parallelism
blockingRisk
submissionAdapter=true|false
```

示例：

```text
task=load-orders
par=database
executor=ThreadPoolExecutor@1a2b
submissionAdapter=true
taskType=IO_BOUND
parallelism=8
blockingRisk=BOUNDED_PLATFORM_POOL
```

指标中的 `parLabel` 是人类可读标签，不保证全局唯一。真实资源关系始终由 executor identity 判断。

## 13. 典型使用

### 13.1 单池

```java
GlobalPar global = GlobalPar.builder()
        .register("default", pool)
        .defaultPar("default")
        .build();
Par par = global.defaultPar();
par.map(items, function, options);
```

### 13.2 多池，共享局部配置

```java
GlobalExecutionPolicy policy = GlobalExecutionPolicy.builder()
        .defaultTimeoutMillis(30_000)
        .build();

GlobalPar global = GlobalPar.builder()
        .executionPolicy(policy)
        .register("database", databasePool)
        .register("http", httpPool)
        .register("cpu", cpuPool)
        .defaultPar("database")
        .build();

Par databasePar = global.par("database");
Par httpPar = global.par("http");
Par cpuPar = global.par("cpu");
```

### 13.3 GlobalPar 一次注册

```java
GlobalPar runtime = GlobalPar.builder()
        .defaultPar("database")
        .deadlockPolicy(GlobalParDeadlockPolicy.builder()
                .enabled(true)
                .listener(deadlockListener)
                .build())
        .purgePolicy(GlobalParPurgePolicy.builder()
                .enabled(true)
                .queuePressureThreshold(0.80)
                .canceledTaskRatioThreshold(0.05)
                .build())
        .register("database", databasePool)
        .register("http", httpPool)
        .register("cpu", cpuPool)
        .build();
```

### 13.4 注入业务组件

```java
OrderLoader orderLoader = new OrderLoader(runtime.par("database"));
RemoteClient remoteClient = new RemoteClient(runtime.par("http"));
```

业务组件内部：

```java
databasePar.map(ids, repository::load, options);
```

### 13.5 不同配置作用域

```java
GlobalPar global = GlobalPar.builder()
        .executionPolicy(policy)
        .parPolicyOverride("database", databasePolicy)
        .parPolicyOverride("cpu", cpuPolicy)
        .register("database", databasePool)
        .register("cpu", cpuPool)
        .build();

Par databasePar = global.par("database");
Par cpuPar = global.par("cpu");
```

`GlobalPar` 内所有 `Par` 使用同一个 `GlobalExecutionPolicy`，必要时使用按 Par 的 override。deadlock 和 purge 仍然只由 GlobalPar 统一配置。

## 14. 现有概念的理想处理

| 现有概念 | 理想处理 |
|---|---|
| `ParConfig.Builder.executor(name, executor)` | 删除 |
| `ParConfig.getExecutor(name)` | 删除 |
| `Par.map(String, ...)` | 删除 |
| 公共 `ExecutionTarget` | 不引入 |
| `ExecutorBinding` | 替换为内部 `ExecutorRuntime` |
| `Par.getInstance()` | 由 `GlobalPar.global().defaultPar()` 取代 |
| `GlobalParConfig` | 合并或替换为 `GlobalPar` 的一次性全局入口 |
| `ThreadRelay.EXECUTOR_NAME` | 改为 executor identity + Par label |
| `TaskEdge.executorName` | 改为 identity + source/target Par label |
| G4 命名注册示例 | 重写为一个 Par 绑定一个执行资源 |

## 15. 实施任务拆解

以下任务按依赖顺序执行。每项都有独立产物和验收条件；未完成前不得进入依赖它的下一项。

### W0：冻结契约与迁移边界（P0）

- 明确 `GlobalPar.Builder.register(...)` 的 null、displayName、shutdown executor 行为。
- 从核心 `Par` API 移除 `bind`；为外部 futures 记录单独的后续提案，不在本次实现中混入。
- 定义 `ExecutorIdentity` 的 reference-equality、生命周期和诊断格式。
- 定义 `GlobalExecutionPolicy`、`BatchExecutionOptions`、`BatchExecutionContext`、`TaskExecutionContext` 的字段白名单和生命周期。
- 将第 18 节不变量转换为实现前置检查清单和回归测试矩阵。
- 产物：API 草案、迁移清单、禁止事项清单。
- 验收：所有公共方法都能回答“哪个 executor 执行”和“谁负责关闭”。

### W1：Par 与 supplied/submission runtime（P0，依赖 W0）

- 新增 `ExecutorRuntime`，在构造期完成普通 executor 的 listening adapter 包装。
- 将 `ParOptions.formalized(...)` 拆为 `BatchExecutionContext.resolve(globalPolicy, options, taskCount)`，不修改用户输入对象。
- 把 submit、类型探测、队列探测、deadlock risk、日志 identity 全部分流到正确对象。
- 删除 `ParConfig` executor registry、`Par.map(String, ...)` 及 `ExecutorBinding` 的公共/内部残留。
- 产物：`Par`、runtime、adapter 行为测试。
- 验收：普通 executor 只包装一次；已是 `ListeningExecutorService` 时不重复包装；adapter 不参与 identity 判断。

### W2：GlobalPar 线程池级 purge 策略与协调器（P0，依赖 W1）

- 在 `GlobalPar.Builder` 增加 `GlobalParPurgePolicy`，移除 `ParConfig` 中 purge enablement/threshold 配置。
- 将 `HeuristicPurger` 的池状态从 `ParConfig` 提升到 `GlobalParPurgePolicy` 的 `PurgeCoordinatorRegistry`。
- registry 以 `ExecutorIdentity` 为 key；同一个 GlobalPar 内同一线程池只建立一个 coordinator。
- 定义策略、禁用策略、失败重试和 borrowed executor 生命周期；不再设计每个 Par 的 purge 策略快照。
- 产物：coordinator、生命周期测试、同一 GlobalPar 内同池多 Par 测试。
- 验收：同一 GlobalPar 内同一 supplied executor 只有一个 `PoolState`；adapter identity 不会产生第二份状态。

### W3：GlobalPar 级请求上下文与任务图（P0，依赖 W1、W4）

- `ThreadRelay` 传播 `ExecutorIdentity` 与 `Par` label；不得传播 runtime/config 对象。
- `TaskEdge` 保存 source/target identity 和 label，不再保存 `ParConfig` 的 deadlock 策略快照。
- `GlobalPar` 创建并绑定 `TaskGraphObservationContext`；`TaskGraph.finishObservation` 只接收该 context。
- 验证跨多个 ExecutorRuntime 的父子 `BatchExecutionContext` 取消传播，以及子 deadline 不超过父 deadline。
- 产物：任务图迁移、同池不同 label、异池同 label、嵌套自环和跨多个 Par 追踪测试。
- 验收：图节点只按 supplied identity 合并；同一 GlobalPar 内所有 Par 的边进入同一张图。

### W4：GlobalPar、跨资源策略与依赖注入边界（P0，依赖 W1）

- 实现不可变 `GlobalPar.Builder`、默认项、重复名称校验、`find/par` 错误模型、`GlobalParDeadlockPolicy` 和 `GlobalParPurgePolicy`。
- 在 build 阶段为所有注册 Par 建立 identity 索引和线程池级 coordinator。
- 静态 global 仅作为一次安装的可选便利入口；核心示例只在 composition root 查找并注入 `Par`。
- 明确测试隔离策略，禁止生产 API 提供 reset/replace/clear。
- 产物：GlobalPar、跨 Par 策略、不可变性/并发安装测试、DI 示例。
- 验收：业务组件只持有 `Par`；构建完成后 map 不可修改；缺失 default/name 有稳定异常。

### W5：Java 8 核心与 Java 21 能力扩展（P1，依赖 W1）

- 在 Java 8 核心中引入 `ExecutorCapabilityProvider` 和 `UNKNOWN` 保守能力。
- 将虚拟线程 provider 与示例放入独立 Java 21 profile/module，避免 Java 8 源码链接 Java 21 API。
- 验证 virtual-thread executor 的取消、超时、parallelism 和任务图语义与普通 executor 一致。
- 产物：SPI、Java 8 编译门槛、Java 21 profile 测试。
- 验收：Java 8 主源码可编译；Java 21 provider 可选加载；未知 executor 不误判为可安全阻塞。

### W6：迁移、文档与发布门槛（P1，依赖 W2-W5）

- 更新 README、G4 示例、demo 和中英文指南，删除字符串注册表选择 executor 的叙事。
- 提供旧 API 到新 API 的迁移表；标出 breaking changes，不承诺本设计中的兼容性。
- 运行 Java 8 编译、完整测试、Javadoc、静态分析和关键并发测试。
- 产物：迁移指南、完整示例、验证报告。
- 验收：文档中的每个代码片段都对应当前 API；测试覆盖 adapter identity、purger sharing、multi-config observation、GlobalPar immutability。

### 推荐执行批次

```text
批次 A：W0
批次 B：W1
批次 C：W2 + W4（可并行）
批次 D：W3 + W5（可并行，W3 在 W4 后接入）
批次 E：W6
```

## 16. 验收标准

### API

- 最小示例只需要 `GlobalPar.builder().register(...)`、注入 `Par` 和 `par.map(...)`。
- `Par.map` 不存在 executor 或 executor name 参数。
- `GlobalPar` 可以一次注册多个 `Par` 并提供可选 defaultPar。
- `GlobalPar` 构建后不能修改。
- 业务组件可以只依赖 `Par`。
- 核心 `Par` API 不暴露任意外部 future 的 `bind` 语义。
- `BatchExecutionOptions` 只包含一次调用的用户输入，不包含 cancellation、executor 或全局策略。
- `Par.map` 为每次调用创建独立的 `BatchExecutionContext`。

### 执行器边界

- 普通 `ExecutorService` 被包装一次供提交使用。
- 用户传入 `ListeningExecutorService` 时不重复包装。
- supplied 和 submission executor 的职责清晰可测试。
- adapter identity 不参与任务图、purge 或 deadlock 判断。
- 框架不关闭 borrowed executor。
- 同一 GlobalPar 内同一 supplied executor 共享一个 purge coordinator。

### 多 Par

- 所有注册到同一 `GlobalPar` 的 `Par` 使用同一个 `GlobalExecutionPolicy`，可选按 Par override。
- 同一 supplied executor 对应同一任务图资源节点。
- 不同 supplied executor 即使 Par label 相同也不合并。
- 嵌套调用使用显式引用的 `Par`。
- 同一 GlobalPar 内多个 Par 的请求使用同一个 `TaskGraphObservationContext`。

### GlobalPar

- deadlock detection 只由 `GlobalParDeadlockPolicy` 配置和分发。
- purge enablement/threshold 只由 `GlobalParPurgePolicy` 配置，状态按 supplied executor 维度维护。
- `GlobalPar` 内所有注册 Par 的 deadlock 边进入同一张请求级任务图。
- 重复名称 build 失败。
- 不存在的名称查找失败并包含名称。
- defaultPar 缺失时行为明确。
- 静态 global 只能安装一次。
- 显式 `GlobalPar` 不依赖静态全局状态。
- `GlobalPar` 明确拥有 deadlock policy、purge policy、ExecutorRuntime 和 observation context 生命周期。
- `GlobalPar.close()` 只关闭框架自己创建的运行时资源，不关闭 borrowed executor。

### 兼容性

- Java 8 主源码不引用 Java 21 API。
- Java 21 虚拟线程能力通过可选 provider 注入，未知 executor 使用保守 `BlockingRisk`。

## 17. 最终模型

```text
应用组装层
    │
    ▼
GlobalPar (immutable)
    ├─ defaultPar
    └─ Map<String, Par>
    ├─ GlobalParDeadlockPolicy
    ├─ GlobalParPurgePolicy
    │    └─ Map<ExecutorIdentity, ExecutorRuntime>
    └─ TaskGraphObservationContext factory
              │
              ▼
Par
    └─ ExecutorRuntime
          ├─ suppliedExecutor
          ├─ submissionExecutor
          ├─ supplied ExecutorIdentity
          ├─ displayName
          ├─ BlockingRisk
          └─ phaseObserver

BatchExecutionOptions
    └─ BatchExecutionContext
          ├─ CancellationToken
          ├─ deadline
          ├─ AsyncBatchResult
          └─ TaskExecutionContext(s)
```

三个核心作用域彼此独立，但跨资源策略归 GlobalPar：

```text
GlobalExecutionPolicy：GlobalPar 的全局默认策略与 Par override
Par：      执行器作用域
GlobalPar：应用组装 + 跨 Par 观测 + 线程池级维护
```

最终原则：

1. 一个 `Par` 稳定绑定一个执行资源。
2. `GlobalPar` 只在构建期注册 `Par`，运行期只读，并拥有跨 Par 的 deadlock 策略。
3. 业务代码依赖 `Par`，不依赖字符串注册表。
4. supplied executor 是真实资源；submission executor 是技术适配器。
5. 名称用于查找和诊断，但不用于资源 identity。
6. 父子批次共享取消链，子 deadline 不得超过父 deadline。

## 18. 设计不变量与变更护栏

以下规则是本设计的硬约束。后续实现、重构或 API 扩展都必须保持这些不变量；如果某项需求无法满足，应先修改设计并更新对应验收测试，不能通过局部兼容逻辑绕开。

### 18.1 作用域不变量

1. `GlobalPar` 是跨 `Par` 的唯一策略和运行时 owner。
2. `Par` 是稳定的逻辑执行入口，不保存任何单次批量任务状态。
3. `BatchExecutionOptions` 只包含一次调用的用户输入，不包含 executor、token、future、listener 或 runtime。
4. `BatchExecutionContext` 只属于一次批量调用；批次结束后必须可释放，不能缓存回 `Par` 或 `GlobalPar`。
5. `TaskExecutionContext` 只属于一个元素任务；任务结束后必须清理线程本地状态。

### 18.2 执行器 identity 不变量

1. supplied executor 是唯一真实资源身份；submission adapter 永远不能成为资源节点、purge key 或 deadlock 判断依据。
2. 同一个 supplied executor 对象在同一个 `GlobalPar` 内只对应一个 `ExecutorRuntime`。
3. 不同 supplied executor 即使类名、displayName 或 identity hash 相同，也不能合并。
4. `identityHashCode` 只用于诊断输出，不能用于相等性判断。
5. 框架不反射解包不透明第三方 wrapper；无法探测的能力必须返回保守值或 `UNKNOWN`。

### 18.3 策略归属不变量

1. deadlock detection 只由 `GlobalParDeadlockPolicy` 开关和 listener 决定。
2. 同一个 `GlobalPar` 内所有注册 `Par` 的任务边进入同一个 observation context。
3. purge enablement、阈值和 coordinator 只由 `GlobalParPurgePolicy` 管理，状态按 supplied executor 维度维护。
4. `TaskType` 只能描述任务行为，不能隐式路由到某个 `Par` 或 executor。
5. 单次 `BatchExecutionOptions` 不能覆盖或修改 GlobalPar 的 deadlock/purge 策略。

### 18.4 取消与 deadline 不变量

1. 取消链只能向下传播：父批次可以取消子批次，子批次不能隐式取消父批次。
2. 跨 executor 的取消传播必须依赖显式 parent token/context，不得依赖某个线程池的 ThreadLocal 恰好可见。
3. 子批次的 effective deadline 不得晚于父批次 deadline。
4. `CancellationToken` 负责取消信号和 future cancellation；deadline 解析属于 `BatchExecutionContext`。
5. 未被框架包装的第三方异步边界必须使用显式 context capture/install，否则不能宣称上下文自动传播。

### 18.5 生命周期与所有权不变量

1. `GlobalPar` build 后不可增删或替换 `Par`。
2. `Par` 不能同时属于两个 `GlobalPar`。
3. borrowed supplied executor 由创建者关闭；`Par`、`GlobalPar` 和 adapter 不得隐式关闭它。
4. `GlobalPar.close()` 只关闭框架自己创建的 timer、purger coordinator 等运行时资源。
5. 请求级 observation context 必须由请求 owner 显式结束，不能由最后一次 `map` 调用猜测请求结束。

### 18.6 API 与观测不变量

1. `Par.map` 不接收 executor、executor name 或公共 `ExecutionTarget`。
2. 业务组件依赖注入得到 `Par`；业务调用期不通过字符串查找 `GlobalPar`。
3. 每次提交的观测字段至少包含 task name、Par label、executor identity、task type、parallelism 和 adapter 标记。
4. Par label 只用于人类可读诊断，不保证唯一；资源关系必须使用 executor identity。
5. Java 8 主源码不能直接链接 Java 21 API；虚拟线程能力只能通过可选 capability provider 注入。

### 18.7 变更前检查清单

任何涉及 `Par`、`GlobalPar`、`BatchExecutionOptions`、`CancellationToken`、`ThreadRelay`、`TaskGraph` 或 `HeuristicPurger` 的改动，至少需要回答：

```text
□ 是否改变了对象的生命周期或 owner？
□ 是否把单次批次状态放进了长期对象？
□ 是否把 adapter 当成了真实 executor？
□ 是否破坏了跨 executor 的父子取消链？
□ 是否让子 deadline 晚于父 deadline？
□ 是否让 deadlock/purge 再次退回 Par 或单个 config？
□ 是否引入了隐式字符串路由或静态全局依赖？
□ 是否需要新增 identity、生命周期、嵌套取消或多 Par 测试？
```

## 19. 变更不变量：防止设计偏移

第 18 节约束运行时行为；本节约束后续改动的方式。任何新增字段、公共方法、策略或兼容层，都必须先归属到一个明确的生命周期和 owner，再实现代码。不能因为某个调用点暂时方便，就把职责下沉到错误的对象。

### 19.1 先判定改动属于哪个作用域

| 改动内容 | 唯一允许的归属 | 不允许的偏移 |
|---|---|---|
| 应用默认值、listener、全局开关 | `GlobalExecutionPolicy` / `GlobalPar` | 放入 `Par` 或 `BatchExecutionOptions` |
| 某次调用的显式要求 | `BatchExecutionOptions` | 写回全局策略或长期缓存 |
| 某次调用解析后的 deadline、token、任务数 | `BatchExecutionContext` | 放入 `Par`、`GlobalPar` 或静态变量 |
| 单个元素的执行信息 | `TaskExecutionContext` | 放入批次对象之外的共享可变状态 |
| supplied executor 的适配、能力和 identity | `ExecutorRuntime` | 以 adapter 作为资源身份 |
| 跨 Par 的 deadlock 观测 | `TaskGraphObservationContext` / `GlobalParDeadlockPolicy` | 在单个 `Par` 内独立检测 |
| 线程池级 purge 状态和阈值 | `GlobalParPurgePolicy` 的 executor identity registry | 按 Par、调用或 adapter 建立状态 |

如果一个改动同时涉及两个作用域，必须明确两者之间的接口，而不是复制一份状态。复制配置或上下文通常意味着产生了第二个事实来源。

### 19.2 公共 API 变更规则

1. 新的 per-call 能力只能通过 `BatchExecutionOptions` 增加；不得重新引入 `ParOptions`、`ExecutionTarget` 或 executor 参数。
2. 新的运行时状态只能通过 `BatchExecutionContext` / `TaskExecutionContext` 暴露；不得把 token、future、deadline 加到 `Par` 的字段中。
3. 新增 executor 相关 API 必须说明 supplied executor、submission executor、identity 和 shutdown owner 分别是什么；缺一项不得合入。
4. `GlobalPar` 的运行期 API 保持只读；任何 `register`、替换或 reset 需求都必须先重新评估不可变拓扑是否仍成立。
5. 兼容旧 API 时，兼容层只能位于边界，并立即转换为新模型；核心实现不得继续依赖旧字符串注册表或旧配置对象。

### 19.3 跨对象改动的同步要求

涉及以下任一行为时，必须同步更新设计文档、验收标准和回归测试：

| 行为 | 至少同步的内容 |
|---|---|
| executor 选择或绑定 | identity 规则、adapter 测试、borrowed ownership 说明 |
| 嵌套 map、取消或超时 | parent/child context 测试、跨 executor 测试、deadline 规则 |
| deadlock 观测 | observation 生命周期、同池/异池图测试、listener 去重规则 |
| purge | supplied identity registry、同池多 Par 测试、close 行为 |
| 公共 API 或命名 | 迁移表、示例代码、Java 8 编译和 Javadoc |

只修改实现而不更新上述契约，视为未完成改动；只修改文档而没有对应测试，视为未验证改动。

### 19.4 合入前的偏移检查

每次结构性改动至少完成以下检查：

```text
□ 新字段是否有唯一 owner，且生命周期与 owner 一致？
□ 是否新增了第二个配置/状态事实来源？
□ 是否混淆 supplied executor 与 submission adapter？
□ 是否改变了父子取消方向或允许子 deadline 延后？
□ 是否把 GlobalPar 级 deadlock/purge 下沉到 Par 或单次调用？
□ 是否让业务调用重新依赖字符串查找、静态 global 或隐式路由？
□ 是否同步更新 API 示例、迁移说明、验收标准和测试矩阵？
□ 是否通过完整测试、Java 8 编译、Javadoc 和关键并发回归？
```

出现任一“是”时，应先停止实现并修正设计归属；不得用额外 boolean、ThreadLocal 或兼容分支掩盖作用域错误。

### 19.5 设计决策记录规则

以下决策属于稳定架构事实，后续变更必须在文档中留下新的决策记录，并说明被替代的不变量：

- `Par` 是否仍然一对一绑定 supplied executor；
- `GlobalPar` 是否仍然是 deadlock/purge 的唯一跨资源 owner；
- cancellation 是否仍然沿显式 parent context 向下传播；
- borrowed executor 的 shutdown owner 是否仍然是创建者；
- Java 8 核心是否仍然与 Java 21 provider 解耦。

没有对应决策记录和回归验证的“临时例外”，不得成为公共 API 或默认行为。
