---
title: "并发库的减法哲学：parallel-in-scope 设计复盘"
date: 2026-06-27
url: ""
views: 0
likes: 0
collects: 0
source: "原创"
---

# 并发库的减法哲学：parallel-in-scope 设计复盘

> 本文记录 v0.2 前后的设计演进，部分代码片段使用历史 API。当前公开 API 以[使用指南](../user-guide.md)和 [v0.2 迁移指南](../migration-v0.2.md) 为准。


> 本文随仓库按 [Apache License 2.0](https://github.com/HuaTalk/parallel-in-scope/blob/main/LICENSE) 许可使用和转载；请保留许可证要求的版权与许可声明。

## 引言

上个月我们的线程池在生产环境卡死了。线程 dump 里没有任何 `BLOCKED` 状态，没有经典的锁死锁。外层任务占住了全部线程等内层完成，内层在队列里排队等外层释放——线程池死锁，一种大多数 Java 开发者从未见过、却随时可能踩到的坑。

排查完这个问题后，我开始重新审视我们写的并发代码。结果发现：取消信号经常石沉大海，ThreadLocal 提交到线程池后神秘消失，超时控制形同虚设。这些问题不是个例，是 Java 并发编程的系统性痛点。

JDK 给了你 `ExecutorService`、`CompletableFuture`、`ForkJoinPool`——工具很多，但没有一个能直接解决上面这三个问题。

**parallel-in-scope** 是我为此写的一个面向 Java 8+ 的结构化并发工具包。它的 API 表面只有一个核心入口：`Par.map`。这篇文章不聊怎么用，聊的是它背后的设计取舍。

在开始之前，先提一条贯穿全文的原则：

> **潜龙勿用。** 并发会引入大量的复杂度——取消传播、上下文丢失、死锁、竞态。只在真正需要并行的场景才引入它。如果你的列表只有 3 个元素，一个 for 循环就够了。

这条原则决定了 parallel-in-scope 的所有设计取舍：**一个并发库的价值，不在于它提供了什么，而在于它拒绝了什么。**

---

## 一、并发痛点与陷阱

### 1.1 取消不了的线程

```java
ExecutorService pool = Executors.newFixedThreadPool(10);
List<Future<String>> futures = urls.stream()
    .map(url -> pool.submit(() -> fetch(url)))
    .collect(Collectors.toList());

// 某个任务失败了，想取消其余的
futures.forEach(f -> f.cancel(true)); // 真的能取消吗？
```

`cancel(true)` 底层调用的是 `Thread.interrupt()`。但如果你的 `fetch()` 方法内部用的是 `HttpClient`、`JDBC`、或者任何不检查中断标志的代码——中断信号石沉大海，线程继续跑。

更麻烦的是嵌套并行：外层任务取消了，内层提交的子任务还在默默执行。取消信号无法自动向下传播。

### 1.2 丢失的上下文

```java
// 请求入口
MDC.put("traceId", traceId);
UserContext.set(currentUser);

// 提交到线程池后……
pool.submit(() -> {
    MDC.get("traceId");  // null！
    UserContext.get();    // null！
});
```

`ThreadLocal` 在线程池中丢失，这是老生常谈的问题。开发者被迫在每个任务函数里手动传参。一个真实项目中，我见过 `fetch()` 的签名从 `fetch(url)` 膨胀到了 `fetch(url, traceId, userId, orgId, timeout, cancellationToken, retryPolicy)`——7 个参数，其中 6 个是"管道"信息，跟业务逻辑毫无关系。

### 1.3 看不见的死锁

```java
// 外层：10 个任务提交到 FixedThreadPool(10)
par.map("shared-pool", orders, order -> {
    // 内层：每个任务又提交 5 个子任务到同一个线程池
    par.map("shared-pool", order.getItems(), item -> process(item), opts);
    return order;
}, opts);
```

外层 10 个任务占住了线程池的全部 10 个线程，等待内层完成。内层的任务在队列里排队，等外层释放线程。经典死锁？不，线程 dump 里看不到任何 `BLOCKED` 状态——因为这不是锁的死锁，是**线程池的死锁**。生产环境偶发卡死，极难复现和定位。

> 这三个问题不是"高级话题"，是每个用线程池的 Java 开发者都可能踩的坑。但 JDK 没有给你开箱即用的方案，CompletableFuture 也没有。

面对这些痛点，parallel-in-scope 的核心设计可以用三句话概括：

> **一个 scope，要么全成功，要么第一个失败时停止。** ——Fail-fast 是唯一语义。
>
> **取消不只是中断线程，还要告诉它为什么。** ——双异常策略。
>
> **上下文传播应该是透明的，业务代码不该知道自己在被并行执行。** ——两 Map 接力。

下面逐一展开。

---

## 二、一把刀：Par.map

如果并发编程的痛苦来自"选择太多"，那解法就是**减少选择**。

```java
AsyncBatchResult<String> result = par.map(
    "io-pool",   // 线程池名称
    urls,        // 输入列表
    url -> fetch(url),  // 处理函数
    options      // 配置
);
```

一个方法，四个参数。线程池名（不是 Executor 对象，后面会解释为什么）、输入列表、处理函数、配置。读完签名就知道怎么用。

**对比 CompletableFuture：** 同样是并行处理 100 个 URL：

```java
// CompletableFuture 的做法
List<CompletableFuture<String>> futures = urls.stream()
    .map(url -> CompletableFuture.supplyAsync(() -> fetch(url), pool))
    .collect(Collectors.toList());

CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

// 超时？手动加（orTimeout 是 Java 9+ API，Java 8 需要自己用 ScheduledExecutorService 实现）
all.orTimeout(30, TimeUnit.SECONDS).join();

// 取消？第一个失败时取消其余的？自己实现
```

至少 15 行样板代码，而且超时控制、失败取消、上下文传播全部要自己处理。`orTimeout` 还是 Java 9+ 才有的 API——Java 8 用户连这行都写不出来，得自己用 `ScheduledExecutorService` + `completeExceptionally` 手搓一个。

**对比 invokeAll：**

```java
// 一次性提交 1000 个任务
List<Future<String>> results = pool.invokeAll(callables); // 阻塞到全部完成
```

`invokeAll` 有两个问题：一是内存压力——1000 个任务一次性全部提交到队列；二是阻塞——你必须等所有任务完成才能拿到结果，无法逐个处理。

`Par.map` 用**滑动窗口**解决了这两个问题。

打个比方：`invokeAll` 就像一家餐厅把所有预约的客人全塞进大堂——100 个人同时挤进去，谁也坐不下。滑动窗口则是翻台制：大堂只有 10 个桌子（parallelism=10），客人走一桌才进一桌。大堂永远满座，但永远不会拥挤。

> **翻台制，不是排队制。** 完成一个补一个，线程池永远满载但不溢出。好的 API 不是让你"可以"做某事，而是让你"只能"做正确的事。

---

## 三、晚绑定：一个反直觉的并发原语

大多数框架在创建 Future 时就绑定超时和取消。parallel-in-scope 先创建完整的结果 Future 列表（未提交任务使用占位 Future），再统一绑定取消和超时。

为什么？

**直觉做法的问题：**

```
任务 1 创建 → 绑定超时 3s
任务 2 创建 → 绑定超时 3s
...（还在创建任务 3-100）
任务 1 超时 → 触发取消 → 任务 3-100 还没提交就被冤杀了
```

滑动窗口的逻辑被破坏了——你本来想"完成一个补一个"，结果变成了"超时一个杀全部"。

**晚绑定的做法（简化示意）：**

```java
// 阶段 1：启动滑动窗口提交
// 前 parallelism 个任务立即提交，剩余任务用 SettableFuture 占位并按完成情况补充
// 每完成一个任务，从队列中取下一个占位符，用 SettableFuture.setFuture() 补充实际任务
AsyncBatchResult<?> result = ConcurrentLimitExecutor
    .create(executor, options, submitterPool)
    .submitAll(wrappedTasks);

// 阶段 2：绑定结果 Future、提交循环、超时、fail-fast 和父级取消
cancellationToken.lateBind(
    result.getResults(), options.forTimeout(), result.getSubmitCanceller());
```

> 注：以上省略了泛型和周边配置。实际实现通过内部 `ListenableCompletionService` + `SettableFuture` 占位 + 独立的 `submitterPool` 阻塞循环完成滑动窗口调度；`submitCanceller` 用于在取消时终止后续任务提交。

`lateBind()` 依次绑定三条链路：

1. **父级取消传播** — 如果存在 parent token，监听 parent 的取消事件，级联取消
2. **Fail-fast** — `Futures.allAsList(futures)` 将所有子 Future 绑定在一起，任一失败立即触发取消
3. **超时** — `FluentFuture.withTimeout()` 在全局定时器上设置超时

**晚绑定的本质是显式分离"提交调度"和"取消绑定"。** `submitAll()` 返回按输入顺序排列的 Future（尚未实际提交的任务由占位 Future 表示），随后再把这些 Future、提交循环和超时连接到同一个取消令牌。

> **提交调度与取消绑定各负其责。** 结果 Future 列表先完整建立，任务仍按滑动窗口逐步提交；取消令牌同时覆盖已提交任务、占位 Future 和提交循环。

> 并发编程中最危险的不是竞态条件，而是"看起来没问题但时序依赖巧合"的代码。晚绑定消除了这类隐式依赖。

---

## 四、双异常：零开销与可调试性的平衡

晚绑定解决了"什么时候取消"的问题。但取消之后呢？你需要告诉任务"你被取消了"——这就是检查点的职责。

**传统做法的问题：** `Thread.interrupt()` 只设置一个 boolean 标志。你只知道"我被取消了"，不知道是哪个子任务失败触发的 fail-fast、还是整批超时、还是父任务级联传播。而且在高频循环中，你需要不断检查 `Thread.interrupted()`——这个检查本身不花钱，但如果你要在检查到中断时抛异常来中断执行流，`new InterruptedException()` 的 `fillInStackTrace()` 就是另一回事了。

`fillInStackTrace()` 是 JVM 里最贵的操作之一——它要收集整个调用栈的栈帧，大约占异常创建总耗时的 90%+，一次调用约 1-5 微秒（取决于栈深度）。如果检查点每秒被调用 100 万次，那就是每秒 1-5 秒的 CPU 时间花在填栈上——这个开销是致命的。

parallel-in-scope 在高频路径提供轻量取消异常，并在诊断场景使用 JDK 标准异常：

- **`LeanCancellationException`：** 覆写 `fillInStackTrace()` 返回 `this`，零开销。用于高频检查点。
- **`CancellationException`：** 保留完整堆栈。用于调试场景。

```java
// 典型使用位置：循环体内部或长任务的关键步骤之间
for (Item item : items) {
    Checkpoints.checkpoint("process-item", true);  // 零开销，抛出 LeanCancellationException
    process(item);
}

// 调试时切换为 false，保留完整堆栈方便排查
Checkpoints.checkpoint("process-item", false); // 抛出 CancellationException
```

对比 `Thread.interrupt()`：中断标志是一个 boolean，你不知道是谁取消的、为什么取消。

`CancellationToken.State` 用带符号的 int code 区分取消原因（以下列出取消相关状态，省略了 `SUCCESS` 和 `NO_OP`）：

| 状态 | Code | 含义 |
|---|---|---|
| `RUNNING` | 0 | 正常执行 |
| `FAIL_FAST_CANCELED` | -1 | 某个子任务失败，触发 fail-fast |
| `TIMEOUT_CANCELED` | -2 | 批次超时 |
| `MUTUAL_CANCELED` | -3 | 多个取消源同时触发 |
| `PROPAGATING_CANCELED` | -4 | 父任务取消，级联传播 |

`shouldInterruptCurrentThread()` 方法简单判断 `code < 0`——如果是负数，说明被取消了，检查点立即抛出异常。

> 好的并发框架不仅要告诉你"任务被取消了"，还要告诉你"为什么被取消"。

---

## 五、SmartBlockingQueue：CPU 密集型任务的"反队列"策略

解决了取消和超时之后，还有两个横切关注点需要处理：任务怎么排队（CPU 任务和 IO 任务的策略不同），以及上下文怎么跨线程传播。先说排队。

**传统做法的问题：** 任务来了就入队，一视同仁。但想象一个场景：线程池有 10 个线程，队列容量 100。提交了 50 个 CPU 密集型任务，全部入队。接下来提交 20 个 IO 密集型任务——排在 CPU 任务后面。IO 任务本应很快完成（只是等网络响应），CPU 任务要算很久。结果 IO 任务被堵住，延迟飙升。

CPU 任务排队只增加延迟，不增加吞吐。

parallel-in-scope 的做法：CPU 密集型任务，宁可同步执行也不排队。

```java
// CPU 密集型
ParOptions cpuOpts = ParOptions.cpuTask("compute")
    .parallelism(Runtime.getRuntime().availableProcessors())
    .build();

// IO 密集型
ParOptions ioOpts = ParOptions.ioTask("fetchRemote")
    .parallelism(20)
    .timeout(5000)
    .build();
```

`SmartBlockingQueue` 继承 Guava 的 `ForwardingBlockingQueue`，覆写 `offer()` 方法。核心逻辑：

```java
@Override
public boolean offer(E e) {
    BatchExecutionContext batch = SubmissionScope.currentBatch();
    // CPU 密集型任务：拒绝入队，触发 CallerRunsPolicy 同步执行
    if (batch != null && batch.taskType() == TaskType.CPU_BOUND) {
        return false;
    }
    // 显式拒绝入队的场景
    if (batch != null && batch.rejectEnqueue()) {
        return false;
    }
    return delegate.offer(e);  // 组合模式，委托给内部队列
}
```

`offer()` 返回 `false` 会触发 `ThreadPoolExecutor` 的 `RejectedExecutionHandler`（通常配置为 `CallerRunsPolicy`），让调用方线程同步执行该任务。

`SmartBlockingQueue` 的策略是：CPU 任务不入队；其他任务是否入队还取决于 `rejectEnqueue`。返回 `false` 后的实际行为由线程池配置的拒绝策略决定，使用 `CallerRunsPolicy` 时才会在调用方线程执行。

> **CPU 任务排队只增加延迟，不增加吞吐。** 宁可调用方线程同步执行，也不让计算密集型任务堵住 IO 通道。

> 并发控制不是"限制同时执行的任务数"，而是"让正确的任务在正确的时间执行"。

---

## 六、上下文边界：只表达实际执行与实际提交

任务执行时，`TaskExecutionContext.current()` 是当前任务的唯一来源；取消、deadline 和嵌套批次关系都从它的 `batchContext()` 读取。任务尚未开始时没有“当前任务”，线程池只需要知道正在提交哪个批次，内部 `SubmissionScope` 因而只在提交调用的短窗口中存在，用于让 `SmartBlockingQueue` 读取入队策略。

这两个作用域都不通过任意用户线程池提交传播。结构化的子任务必须由 `Par.map()` 创建，避免任意 `Runnable` 被误认为取消树或任务图中的子节点。

---

## 七、死锁检测：不是锁的死锁，是线程池的死锁

回到引言中的那个场景：

```java
par.map("shared-pool", orders, order -> {
    par.map("shared-pool", order.getItems(), item -> process(item), opts);
    return order;
}, opts);
```

外层 10 个任务占住了 `FixedThreadPool(10)` 的全部线程，等待内层完成。内层的任务在队列里排队，等外层释放线程。

这不是传统的锁死锁，线程 dump 里看不到 `BLOCKED` 状态。这是**线程池的死锁**——线程在 `Future.get()` 上等待，而不是在 `synchronized` 上等待。

parallel-in-scope 的解决方案是 **请求级 DAG 图**（DAG = Directed Acyclic Graph，有向无环图，这里用它记录任务之间的依赖关系）。每次 `Par.map()` 调用时，`logTaskPair()` 记录一条从父任务到子任务的边，边的元数据包含并行度、任务类型、执行器名称、任务数量、超时时间：

```java
// 请求入口
try (TaskGraphObservationContext observation = global.openTaskGraphObservation()) {
    // ... 执行业务逻辑，期间所有 Par 调用会自动记录依赖关系
    // 例如上面的嵌套调用会记录两条边：
    //   "processOrder" → "fetchItem"  (executor: shared-pool → shared-pool)
} // 请求结束时 close() 自动检测
```

请求结束时执行两层检测：

1. **任务级** — 在任务名的图上检测环路（`Graphs.hasCycle()`）和自环。上面的例子中，"processOrder → fetchItem → processOrder" 形成环路，立即检出。
2. **执行器级** — 将任务图提升为执行器图，只关注"可能死锁"的执行器。

`canDeadlock()` 方法智能判断执行器是否可能死锁：

- 使用 `SynchronousQueue` 的执行器（如 `CachedThreadPool`）→ 不会死锁，因为 `SynchronousQueue` 不缓冲任务，提交即执行
- `maximumPoolSize` 为 `Integer.MAX_VALUE` 的执行器 → 不会死锁，因为线程数可以无限增长
- 其他情况（如 `FixedThreadPool` + `LinkedBlockingQueue`）→ **可能死锁**

检测结果通过 `DeadlockDetectionListener` SPI 回调通知：

```java
ParConfig config = ParConfig.builder()
    .executor("shared-pool", pool)
    .deadlockDetectionEnabled(true)
    .deadlockListener(event -> {
        if (event.hasExecutorSelfLoop()) {
            log.warn("Potential deadlock: executor self-loop detected! {}",
                event.getExecutorEdges());
        }
    })
    .build();
```

> 死锁检测不应该是事后分析工具，应该是运行时守护。问题在发生的瞬间就被发现。

---

## 八、IdeaGraveyard：比代码更重要的设计文档

讲完了 parallel-in-scope 的核心机制，现在可以聊聊它**拒绝了什么**。

每个开源项目都有一个 TODO list，记录计划要做的功能。parallel-in-scope 有一个反向的文档：**IdeaGraveyard**——记录所有被认真评估、但最终决定不实现的特性。参考的是 [Guava 的同名实践](https://github.com/google/guava/wiki/IdeaGraveyard)。

挑几个最有意思的：

### 可配置失败策略（Failure Policy）

**请求：** 提供 `failFast(false)` 选项，让某些子任务失败后其余任务继续执行。

**为什么拒绝：** 我们曾经实现过 `failFast` 开关，后来主动删除了它。

fail-fast 是结构化并发的核心语义。允许"忽略失败继续跑"会引入连锁问题：结果模型爆炸（调用方需要区分"成功的结果"和"失败的占位符"）、取消语义模糊（`CancellationToken` 的级联逻辑需要分裂成两条路径）、隐藏真正的问题（继续执行只是在浪费资源）。

**替代方案：** 在任务函数内部 catch 异常，返回 `Optional<T>`：

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

只有业务代码知道哪些异常可以忽略、怎样降级。框架不该替你做这个决定。

### 内置重试（Retry）

**请求：** 内置重试机制，如 `ParOptions.retry(3).backoff(100, MILLISECONDS)`。

**为什么拒绝：** 重试是一个**策略密集型**问题——重试哪些异常？退避策略？幂等性？重试时是否占用并发窗口？每个决策点都是业务相关的。

更根本的问题是：**重试和 fail-fast 存在语义冲突。** 一个任务失败后到底是立即取消同伴还是先重试？这两个语义会打架。

**替代方案：** 在任务函数内部用专门的重试库（Resilience4j、Failsafe、Spring Retry）。

### Spring Boot Starter

**请求：** 提供 `parallel-in-scope-spring-boot-starter`，通过 `@Parallel` 注解声明并行任务。

**为什么拒绝：** `@Parallel` 注解看起来很酷，但它隐藏了关键的并发参数（并行度、超时、任务类型），让开发者在不理解底层行为的情况下使用并发工具。

这是真正的危险所在。一个 `@Parallel` 注解意味着开发者不需要思考"这个任务是 CPU 密集还是 IO 密集"、"并行度设多少合理"、"超时多久合适"——这些恰恰是并发编程中最需要思考的问题。注解的便利性会制造一种虚假的安全感：你以为框架帮你处理好了一切，实际上你只是把问题藏起来了。

而且注册机制已经够简单了——10 行 `@Configuration` 代码，不需要框架级集成。

### CompletableFuture 作为返回类型

**请求：** 用 `CompletableFuture` 替代 Guava `ListenableFuture`。

**为什么拒绝：** CF 的 `thenApplyAsync` 不指定 executor 时默认用 `ForkJoinPool.commonPool()`。如果你的回调是 IO 操作，它会污染公共池。你需要用 `thenApplyAsync(callback, executor)` 才能指定线程池——但多少人会记得加那个 executor 参数？

`ListenableFuture` 的 `addListener(Runnable, Executor)` 强制指定回调执行的线程池，第二个参数是必填的。API 设计层面就不会犯这个错。

> 每个被拒绝的特性都有一个"看起来很合理"的请求。但框架的职责不是满足所有请求，而是维护一致的语义模型。看完前面几章的机制设计，你应该能理解为什么这些"不做"的决定是必要的。

---

## 结语：什么是不该做的

全文的设计取舍，可以浓缩为五条原则：

1. **潜龙勿用。** 并发会引入大量复杂度，只在真正需要时才引入。
2. **一个 scope，要么全成功，要么第一个失败时停止。** Fail-fast 是唯一语义。
3. **提交是提交，绑定是绑定。** 不要在上菜之前开酒。
4. **取消不只是中断线程，还要告诉它为什么。** 双异常策略。
5. **CPU 任务排队只增加延迟，不增加吞吐。** 宁可同步执行也不堵 IO。

这些不是最佳实践，是设计约束。一旦在某条上让步，整个设计就会像多米诺骨牌一样坍塌。

parallel-in-scope 有一些已知的局限，它们是刻意的设计选择：

- 不支持流式输入——滑动窗口需要预知总量，流式输入和背压是另一种流控范式
- 不支持异构任务组合——`map(list, function)` 假设同构输入，异构任务用 Guava 原生 API 更合适
- 不支持链式编排——它是批量执行器，不是异步编排框架，编排交给 Guava 或 CF

这些局限不是"还没做"，是"决定不做"。每个"不做"都维护了语义的一致性：fail-fast 是唯一语义、滑动窗口需要预知总量、同构假设支撑了类型安全和监控模型。

回到开头那个线程池死锁的问题。修完之后我问自己：这些并发代码里，有多少行是在解决"选工具"的问题，而不是在解决业务问题？

答案是：太多了。

> 最好的并发框架不是让你能做更多事的框架，而是让你把精力花在业务上，而不是花在跟并发原语搏斗上。

潜龙勿用——但如果用了，就用对。

如果你的项目也有线程池死锁的困扰，试试在测试环境开启 `deadlockDetectionEnabled(true)`——一行配置，零代码改动。至少，下次卡死的时候你能知道为什么。

---

*如果这篇文章对你有帮助，欢迎关注我，持续分享高质量技术干货。*
