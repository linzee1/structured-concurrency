# Idea Graveyard

> 本文部分示例保留 v0.2 前的历史 API。当前应用装配使用 `GlobalPar.builder()` 和 `MultiTaskOptions`，请以 [v0.2 迁移指南](../migration-v0.2.md) 为准。


> 本页记录了我们**认真考虑过但最终决定不实现**的特性，以及拒绝的理由。
>
> 如果你打算为 parallel-in-scope 提一个 feature request，建议先看看这里——你的想法可能已经被讨论过了。

---

## 可配置的失败策略（Failure Policy）

**请求：** 提供 `failFast(false)` 或 `FailurePolicy.CONTINUE` 选项，让某些子任务失败后其余任务继续执行。

**为什么不做：**

我们曾经实现过 `failFast` 开关，后来在 [0b8cb63](https://github.com/huatalk/parallel-in-scope/commit/0b8cb63) 中**主动删除**了它。

fail-fast 是结构化并发的核心语义：一个 scope 内的任务要么全部成功，要么在第一个失败时取消剩余任务。允许"忽略失败继续跑"会引入多种复杂性：

1. **结果模型爆炸。** 调用方需要区分"成功的结果"和"失败的占位符"，`AsyncBatchResult` 的类型签名和使用方式都会复杂化。
2. **取消语义模糊。** 如果允许部分失败继续，那么 `CancellationToken` 的级联逻辑、`allAsList` 的语义都需要分裂成两条路径。
3. **隐藏了真正的问题。** 如果一个批次中有任务失败，通常意味着整个批次的结果已经不完整，继续执行只是在浪费资源。

**替代方案：** 在你的任务函数内部 catch 异常，返回一个包装类型（如 `Either<Error, T>` 或 `Optional<T>`），这样每个任务永远不会"失败"——框架看到的全是成功：

```java
par.map("io-pool", urls, url -> {
    try {
        return Optional.of(httpClient.fetch(url));
    } catch (Exception e) {
        log.warn("fetch failed: {}", url, e);
        return Optional.empty();
    }
}, options);
```

这比框架内置失败策略更清晰，因为只有业务代码知道哪些异常可以忽略、怎样降级。

---

## 任务编排 / 链式组合（thenApply, thenCompose, exceptionally...）

**请求：** 像 `CompletableFuture` 那样支持链式编排：`map(...).thenApply(...)`, `exceptionally(...)`, 或者更高级的 DAG 编排（A 和 B 并行，C 依赖 A+B）。

**为什么不做：**

parallel-in-scope 是一个**批量并行执行器**，不是通用异步编排框架。这两者的设计目标是正交的：

- **批量执行器**关注的是：并发控制、取消传播、上下文传递、监控。输入是同构列表，输出是同构结果。
- **异步编排框架**关注的是：任务间的依赖关系、异常恢复、结果变换。输入是异构任务图。

Java 生态已经有成熟的编排方案：`CompletableFuture`、Guava `FluentFuture`/`Futures.transform()`、[cffu](https://github.com/foldright/cffu)。我们没有理由重新造这些轮子。如果 parallel-in-scope 也提供 `thenApply`，用户面对的是两套风格不同的 Future 链，反而增加心智负担。

**替代方案：** 把 parallel-in-scope 当作编排链中的一个"节点"——它负责批量执行，编排交给 Guava 或 CF：

```java
// 阶段 1：批量获取
AsyncBatchResult<Price> prices = par.map("io-pool", skuList, this::fetchPrice, opts);

// 阶段 2：用 Guava 编排后续逻辑
ListenableFuture<List<Price>> allPrices = Futures.allAsList(prices.getResults());
ListenableFuture<Report> report = Futures.transform(allPrices, this::buildReport, executor);
```

---

## 重试（Retry）

**请求：** 内置重试机制，如 `ParOptions.retry(3).backoff(100, MILLISECONDS)`。

**为什么不做：**

重试看起来简单，但它是一个**策略密集型**问题——每个决策点都是业务相关的：

| 决策点 | 选项 |
|---|---|
| 重试哪些异常？ | 所有异常？仅 IOException？排除 IllegalArgumentException？ |
| 退避策略？ | 固定间隔？指数退避？抖动？ |
| 幂等性？ | 重试一个 HTTP POST 安全吗？ |
| 重试时是否占用并发窗口？ | 占用则降低吞吐，不占用则可能超出并发限制 |
| 重试的结果算成功还是另一种状态？ | 影响 `BatchReport` 的统计语义 |

把这些策略内置到框架中，要么做得太简单（覆盖不了实际场景），要么做得太复杂（变成了 Resilience4j）。无论哪种，都偏离了 parallel-in-scope 的定位。

更根本的问题是：**重试和 fail-fast 存在语义冲突。** parallel-in-scope 承诺"任一任务失败立即取消整批"。如果引入重试，一个任务失败后到底是立即取消同伴还是先重试？这两个语义会打架。

**替代方案：** 在任务函数内部实现重试，或者使用专门的重试库（Resilience4j、Failsafe、Spring Retry）：

```java
par.map("io-pool", urls, url -> {
    return retryPolicy.execute(() -> httpClient.fetch(url));
}, options);
```

---

## 任务优先级（Intra-Batch Priority）

**请求：** 支持批次内任务的优先级排序，让重要的任务先执行。

**为什么不做：**

`ParOptions` 曾经有一个 `priority` 字段（已移除），原本设想用于 `SmartBlockingQueue` 的**批次间调度**（多个 `map` 调用竞争同一个线程池时，高优先级批次的任务先出队），但从未实际使用，因此被清理掉了。

批次内优先级不实现的原因：

1. **同构假设。** `map(list, function)` 对列表的每个元素应用同一个函数——这些任务在语义上是等价的，谁先谁后不应该影响最终结果。如果某些元素确实比其他重要，说明它们不该在同一个批次里。
2. **滑动窗口语义冲突。** 当前的滑动窗口按列表顺序提交，简单可预测。引入优先级需要一个 PriorityQueue，增加复杂度，而且在 fail-fast 语义下意义有限——整个批次要么全成功，要么在第一个失败时取消。
3. **调试困难。** 按列表顺序执行，出问题时 `index=42 的任务失败了` 很容易定位。引入优先级后，执行顺序不再与输入顺序对应，排查难度上升。

**替代方案：** 如果不同元素有不同的紧急程度，拆成多个批次分别调用；或者在提交前对列表排序。

---

## Spring Boot Starter 集成

**请求：** 提供 `parallel-in-scope-spring-boot-starter`，自动注入线程池、通过 `@Parallel` 注解声明并行任务。

**为什么不做：**

1. **依赖独立性。** parallel-in-scope 的核心价值之一是**不依赖应用框架**（运行时依赖仅为 Guava 和 TTL）。引入 Spring 依赖意味着一个 `spring-boot-starter` 模块需要 Spring Boot 2.x/3.x 兼容矩阵、autoconfiguration、条件装配——维护成本远高于核心功能。
2. **注册机制已经够简单。** `ParConfig.builder().executor("name", executor)` 可在构建配置时完成注册。在 Spring 项目中，写一个 `@Configuration` 类即可，不需要框架级集成。
3. **注解魔法的危害。** `@Parallel` 注解看起来很酷，但它隐藏了关键的并发参数（并行度、超时、任务类型），让开发者在不理解底层行为的情况下使用并发工具——这与 parallel-in-scope "显式优于隐式"的设计哲学相悖。

**替代方案：** 在 Spring 项目中手动配置（约 10 行代码）：

```java
@Configuration
public class ParallelInScopeConfig {
    @Bean
    public Par parallelExecutor(
            @Qualifier("ioPool") ExecutorService ioPool,
            @Qualifier("cpuPool") ExecutorService cpuPool) {
        ParConfig config = ParConfig.builder()
            .executor("io-pool", ioPool)
            .executor("cpu-pool", cpuPool)
            .build();
        return new Par(config);
    }
}
```

---

## Per-Task Timeout（单任务超时）

**请求：** 除了批次级超时，还能为单个任务设置独立的超时时间。

**为什么不做：**

parallel-in-scope 只提供批次级超时（`ParOptions.timeout()`），不提供单任务超时。原因是两者在 fail-fast 语义下几乎等价，但实现复杂度差异巨大：

- **批次超时：** 一个 `FluentFuture.withTimeout()` 搞定，语义清晰——"整批任务最多跑 N 秒"。
- **单任务超时：** 每个任务需要独立的 `ScheduledFuture` 来触发取消。在一个 1000 元素的批次中，这意味着 1000 个定时器。而且在 fail-fast 模式下，第一个任务超时就会取消整批——效果和批次超时一样。

唯一的区别出现在"不同任务需要不同超时值"的场景，但这暗示这些任务不是同构的——它们不应该在同一个 `map` 里。

**替代方案：** 在任务内部自行控制超时：

```java
par.map("io-pool", urls, url -> {
    return httpClient.fetch(url, /* requestTimeout = */ Duration.ofSeconds(2));
}, options);
```

---

## CompletableFuture 作为返回类型

**请求：** 用 `CompletableFuture` 替代 Guava `ListenableFuture` 作为 API 返回类型，或者至少提供 CF 适配。

**为什么不做：**

1. **`ListenableFuture` 是更好的并发原语。** 它的 `addListener(Runnable, Executor)` 强制指定回调执行的线程池，避免了 `CompletableFuture` 中臭名昭著的 `ForkJoinPool.commonPool()` 默认行为和 `thenApply` vs `thenApplyAsync` 的混淆。
2. **内部基础设施绑定。** `ConcurrentLimitExecutor` 使用内部 `ListenableCompletionService`、`SettableFuture`、`FluentFuture.withTimeout()`、`Futures.allAsList()` 等 Guava 原语构建——这些是核心机制，不是可替换的表面 API。提供 CF 适配层意味着维护两套 Future 语义。
3. **互操作可以显式完成。** Guava 没有内置 `ListenableFuture` 到 `CompletableFuture` 的转换 API；如果调用方确实需要 CF，可以在应用边界通过 `FutureCallback` 完成一个小型适配器，或选用专门维护的互操作库。

---

## Streaming / Reactive 输入（Flux, Stream, Iterator）

**请求：** 支持 `Stream<T>` 或 `Flux<T>` 作为输入，实现惰性加载 + 并行执行。

**为什么不做：**

parallel-in-scope 要求输入是一个**已物化的 `List<T>`**，这是刻意的：

1. **总量必须提前已知。** `ParOptions.formalized()` 会将并行度 clamp 到 `min(parallelism, taskSize)`——如果不知道总量，无法做这个优化。`BatchReport` 的状态统计也依赖于预知总任务数。
2. **滑动窗口需要随机访问。** `ConcurrentLimitExecutor.submitAll()` 按索引提交任务，`SettableFuture` 按索引占位。流式输入无法提供这种随机访问模式。
3. **背压语义冲突。** 响应式流的背压机制和滑动窗口是两种不同的流控范式。让它们共存在同一个执行模型中会互相干扰，语义变得不可预测。

**替代方案：** 先收集再提交。如果数据源是流式的，在入口处物化为列表：

```java
List<Item> items = stream.collect(Collectors.toList());
par.map("io-pool", items, this::process, options);
```

---

## 异构任务组合（invokeAll for Different Callables）

**请求：** 支持同时提交不同类型的任务，如 `Par.invokeAll(fetchUser, fetchOrder, fetchInventory)`，每个返回不同类型。

**为什么不做：**

parallel-in-scope 的 API 签名是 `map(List<T>, Function<T, R>)` ——输入同构，输出同构。这不是偶然的：

1. **类型安全。** Java 泛型不支持异构列表的类型安全返回。`invokeAll(Callable<A>, Callable<B>, Callable<C>)` 的返回类型只能是 `List<Future<?>>` 或者需要大量的重载（2 参数、3 参数、4 参数……直到 N 参数），Guava 和 CF 的做法也只能到此为止。
2. **并发控制无意义。** 异构任务通常只有 2-5 个，不需要滑动窗口、并发限制这些 parallel-in-scope 的核心能力。`Futures.allAsList()` 或 `CompletableFuture.allOf()` 已经足够。
3. **监控粒度不匹配。** parallel-in-scope 的 `TaskListener` 和 `BatchReport` 假设一个批次内所有任务是同名同类型的。异构任务意味着每个任务需要独立的名称、独立的 SPI 回调——这是完全不同的监控模型。

**替代方案：** 用 Guava 原生 API 编排异构任务：

```java
ListenableFuture<User> userF = executor.submit(() -> fetchUser(id));
ListenableFuture<Order> orderF = executor.submit(() -> fetchOrder(id));
ListenableFuture<Inventory> invF = executor.submit(() -> fetchInventory(id));

ListenableFuture<UserProfile> profile = Futures.whenAllSucceed(userF, orderF, invF)
    .call(() -> buildProfile(Futures.getDone(userF), Futures.getDone(orderF), Futures.getDone(invF)),
          executor);
```

---

## 动态并发调整（Adaptive Concurrency）

**请求：** 根据任务延迟、错误率、系统负载自动调整并发窗口大小。

**为什么不做：**

自适应并发（如 Netflix 的 `concurrency-limits` 库）是一个独立的、复杂的领域：

1. **算法选择困难。** Vegas、Gradient、AIMD……每种算法都有适用场景和调参需求。内置任何一种都意味着为所有用户做了选择。
2. **可观测性要求高。** 自适应算法需要持续采样延迟百分位、区分排队延迟和执行延迟、识别上游限流——这些超出了 parallel-in-scope 的职责范围。
3. **可预测性优先。** parallel-in-scope 的定位是"你设了 `parallelism(10)`，它就严格跑 10 个"。这种确定性在排查线上问题时非常宝贵。自适应系统在出问题时，你首先得排查"此刻的并发度是多少、为什么是这个值"——调试难度显著上升。

**替代方案：** 在 parallel-in-scope 外部实现自适应逻辑，动态传入不同的 `parallelism` 值：

```java
int concurrency = adaptiveLimiter.currentLimit();
ParOptions opts = ParOptions.ioTask("fetch").parallelism(concurrency).build();
```

---

## 不会有的东西

以下特性与 parallel-in-scope 的定位有根本冲突，不会被考虑：

| 特性 | 理由 |
|---|---|
| **分布式任务调度** | parallel-in-scope 是 JVM 内工具，不是 Temporal / Airflow |
| **持久化 / 断点续传** | 批量执行是瞬时的，没有状态持久化的需求 |
| **跨进程 Deadline 传播** | 需要 RPC 框架级集成（gRPC deadline），超出作用域 |
| **Work-Stealing 调度** | 用 `ForkJoinPool` 作为底层 executor 即可，不需要重新实现 |
| **Kotlin Coroutine 集成** | Kotlin 有自己的结构化并发（`coroutineScope`），不需要 Java 方案 |
