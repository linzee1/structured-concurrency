# I1. 设计墓地——那些我们拒绝做的功能

每个开源项目都有一个公开的路线图，告诉用户"我们要做什么"。但很少有项目告诉你"我们决定不做什么"。我们反其道而行之——parallel-in-scope 维护了一份 [IdeaGraveyard](https://github.com/huatalk/parallel-in-scope/blob/main/IdeaGraveyard.md)，记录每一个被认真考虑过、最终被拒绝的特性。

这篇文章挑选了 5 个最有代表性的"亡魂"，讲讲它们为什么被拒，以及你该用什么替代方案。

---

## 为什么要有墓地

写代码的人习惯做加法：这个功能不错，加上；那个用户提了需求，加上。但对一个工具库来说，**每一个加进去的功能都是永久的维护负担和认知负荷**。Guava 也有自己的 IdeaGraveyard，我们学的就是这个精神——说"不"比说"是"更难，也更重要。

---

## 被拒绝的特性

### 1. 重试机制

**提议：** 内置 `BatchExecutionOptions.retry(3).backoff(100, MILLISECONDS)`。

**为什么拒绝：** 重试看起来只有一行配置，背后却是一个策略密集型问题。重试哪些异常？指数退避还是固定间隔？重试时占不占并发窗口？幂等性怎么保证？更致命的是，**重试和 fail-fast 语义冲突**——我们承诺"任一任务失败立即取消整批"，如果引入重试，这两个语义会打架。

**替代方案：** 在任务函数内部自己做重试，或者用 Resilience4j、Guava Retryer 等专业库：

```java
par.map(urls, url -> {
    return retryOn(IOException.class, maxRetries(3), () -> httpClient.fetch(url));
}, options);
```

只有业务代码知道哪些异常值得重试、怎样降级。

---

### 2. Spring Boot Starter

**提议：** 提供 `parallel-in-scope-spring-boot-starter`，自动注入线程池，用 `@Parallel` 注解声明并行任务。

**为什么拒绝：** 三个原因。第一，我们核心依赖只有 Guava 和 TTL，引入 Spring 意味着要维护 2.x/3.x 兼容矩阵，成本远高于核心功能本身。第二，`GlobalPar.registerExecutor("name", executor)` 一行代码就够，不值得为此搞一个 Starter。第三，也是最关键的——`@Parallel` 注解会隐藏并行度、超时、任务类型这些关键参数，让开发者在不理解底层行为的情况下使用并发工具，这和我们"显式优于隐式"的哲学相悖。

**替代方案：** 在 Spring 项目里写一个约 10 行的 `@Configuration` 类：

```java
@Bean
public CommandLineRunner registerExecutors(
        @Qualifier("ioPool") ExecutorService ioPool) {
    return args -> {
        // GlobalPar 构建时注册，运行时不可变
        GlobalPar config = GlobalPar.builder()
                .register("io-pool", ioPool)
                .build();
        // 注入到 Spring 容器供全局使用
    };
}
```

---

### 3. 返回 CompletableFuture

**提议：** 用 `CompletableFuture` 替代 Guava `ListenableFuture` 作为 API 返回类型。

**为什么拒绝：** `ListenableFuture` 是更好的并发原语——它的 `addListener(Runnable, Executor)` 强制指定回调线程池，避免了 `CompletableFuture` 默认用 `ForkJoinPool.commonPool()` 的坑。我们的内部实现（`SlidingWindowSubmitter`、`FluentFuture.withTimeout`、`Futures.allAsList`）全部基于 Guava 原语，提供 CF 适配层意味着维护两套 Future 语义。

**替代方案：** 如果调用方确实需要 CF，一行代码转换：

```java
// Guava 原生方式：Futures.transform 转换结果
CompletableFuture<String> cf = new CompletableFuture<>();
Futures.addCallback(lf, new FutureCallback<String>() {
    @Override public void onSuccess(String result) { cf.complete(result); }
    @Override public void onFailure(Throwable t) { cf.completeExceptionally(t); }
}, MoreExecutors.directExecutor());
```

---

### 4. 可配置失败策略

**提议：** 提供 `failFast(false)` 或 `FailurePolicy.CONTINUE`，让部分任务失败后其余继续执行。

**为什么拒绝：** 我们曾经实现过 `failFast` 开关，后来主动删掉了。fail-fast 是结构化并发的核心语义：一个 scope 内的任务要么全部成功，要么在第一个失败时取消剩余任务。允许"忽略失败继续跑"会让结果模型爆炸（调用方要区分成功结果和失败占位符），取消语义变得模糊，而且——如果一个批次中有任务失败，通常说明整个批次结果已经不完整，继续执行只是浪费资源。

**替代方案：** 在任务函数内部 catch 异常，返回包装类型：

```java
par.map(urls, url -> {
    try {
        return Optional.of(httpClient.fetch(url));
    } catch (Exception e) {
        return Optional.empty();
    }
}, options);
```

---

### 5. 自定义线程池工厂 / 动态并发调整

**提议：** 根据任务延迟、错误率、系统负载自动调整并发窗口大小；或者提供线程池工厂 SPI。

**为什么拒绝：** 自适应并发（如 Netflix 的 concurrency-limits）是一个独立的复杂领域，需要持续采样延迟百分位、区分排队延迟和执行延迟、选择 Vegas/AIMD 等算法。更根本的是，**可预测性优先**——parallel-in-scope 的定位是"你设了 `parallelism(10)`，它就严格跑 10 个"。这种确定性在排查线上问题时非常宝贵。自适应系统出问题时，你首先得排查"此刻的并发度是多少、为什么是这个值"。

**替代方案：** 在框架外部实现自适应逻辑，动态传入不同的 `parallelism` 值：

```java
int concurrency = adaptiveLimiter.currentLimit();
MultiTaskOptions opts = MultiTaskOptions.of("fetch").taskType(TaskType.IO_BOUND).parallelism(concurrency)
        .timeout(java.time.Duration.ofMillis(5000)).build();
```

---

## 取舍的智慧

回看这份墓地，被拒绝的特性并非不好——重试、Starter、CF 适配，每一个都有合理的使用场景。但它们不属于 parallel-in-scope。

一个库的边界感决定了它的寿命。功能越多，bug 面越大，API 越难稳定，维护者越容易被拖垮。我们选择只做一件事——**JVM 内的结构化批量并发**——并把它做好。

**一个库的价值不在于它提供了什么，而在于它拒绝了什么。**

---

> 设计墓地完整版：[IdeaGraveyard.md](https://github.com/huatalk/parallel-in-scope/blob/main/IdeaGraveyard.md)
