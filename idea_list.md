# Idea List

> 尚未实现、需要后续详细讨论的想法集合。这里既不是契约文档，也不是
> [idea-graveyard](docs/zh/design/idea-graveyard.md)（已否决的想法）。
>
> 每条 idea 讨论出定论后：要么落成 `design/` 下的契约文档并实现，要么移入
> idea-graveyard 并记下否决理由。状态取值：`待讨论` / `讨论中` / `已采纳` / `已否决`。

---

## 001 只接受 AsyncCallable 作为用户任务

**状态**：待讨论

### 想法

用户传入的任务类型由 `Callable<V>` 收敛为异步形态，例如：

```java
interface AsyncCallable<V> {
    ListenableFuture<V> call() throws Exception;
}
```

名称与形态待定（`AsyncCallable` / 直接在 `Par`、`TaskGroup` 上做适配，都可能）。
核心是：用户交付的不是「返回值」，而是「代表这次工作的 future」。

### 动机

1. **取消能落到传输层。** 现状的取消是 token 自顶向下传播加 `cancel(true)` 中断工作线程，
   属于协作式生效（见 `design/cancellation-propagation.md`）。阻塞在远端调用的任务
   （Redis、HTTP、DB）未必响应中断，取消要等下一个检查点才被观察到，线程在此期间仍被占用。
   如果用户交出的是异步客户端（Lettuce、gRPC、okhttp enqueue、Guava）的 future，框架就能直接
   对它 `cancel(true)`：请求在链路上被真正终止，线程也不再被占住。
2. **把运行时异常翻译成可预期的取消。** 基础设施异常（如 Redis 服务不可用）不再以任意
   `RuntimeException` 泄漏给调用方，而是由框架翻译成「已经取消的 ListenableFuture」并挂到既有的
   取消归因上（`LeanCancellationException`、`TokenOutcomes`）。调用方看到的是统一的取消语义与
   归因，而不是某个第三方客户端的异常类型。
3. **只有一条取消路径。** 不存在「同步任务一套、异步任务一套」的并行语义。

### 影响面（初判）

- **执行内核。** `TaskSubmissions.wrapScoped`、`ScopedCallable`、`ExecutionPhaseHintFuture` 目前
  都以 `Callable<V>` 为中心；异步化后生命周期插桩的边界从「方法返回」变成「future 完成」，
  `ExecutionPhase` 的含义需要重新定义。
- **上下文。** TTL 捕获发生在提交线程；异步完成回调线程上的 `TaskExecutionContext` /
  `TaskGraphObservationScope` 需要显式恢复，否则「上下文不泄漏」这条公理被打破。
- **执行器绑定。** 任务可能跑在用户自己的线程池上，`Par` 绑定的 executor、`ExecutorIdentity`、
  `BlockingRisk`、`HeuristicPurger`、死锁检测的语义都要重新审视。
- **超时。** 可以把超时同时也 `cancel(true)` 到底层 future——这是净收益，不是负担。

### 待讨论

- **同步任务怎么办。** 用户在自己线程上先算完再 `Futures.immediateFuture` 返回，等于同步执行；
  要么就必须自己提交到某个池子。前者退化，后者把 executor 的选择权交还用户，与「用户不管理
  执行器生命周期」的现约定有张力。
- **「只接受」的边界。** 与公理 4（贴近 JDK 习语）是否冲突：`ExecutorService.submit(Callable)`
  是用户心智模型的一部分。终态最优前提下（不迁就兼容性），问题是终态 API 是否还保留任何同步入口、
  它的语义是什么，而不是能否平滑迁移。
- **已取消 future 的归因。** 用户自己返回 cancelled future，与框架取消，必须能区分；
  否则观测在说谎，`TaskOutcome` 需要显式表达这一差别。
- **取消是尽力而为。** `ListenableFuture.cancel` 不保证第三方客户端真的终止请求；契约必须写清
  框架只承诺「发起 cancel 并记账」，不承诺对端行为。
- **签名统一。** `Par.map(List, Function, BatchOptions)` 的 `Function<T, Callable<R>>` 与
  `TaskGroupDefinition.task(TaskKey, ParName, Callable, TaskOptions)` 如何收敛到同一形态。

### 关联

- `design/first-principles.md`（公理 2 忘记、公理 3 安全优先、公理 4 贴近 JDK）
- `design/cancellation-propagation.md`（取消与中断位传播的现状）
- `design/extension-and-wrapping.md`
