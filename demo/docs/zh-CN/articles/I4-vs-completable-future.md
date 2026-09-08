# I4. parallel-in-scope vs CompletableFuture.allOf


## CompletableFuture.allOf 能做什么

`CompletableFuture.allOf()` 是 JDK 标准的批量异步等待原语。它的作用很简单：接收一组 `CompletableFuture`，返回一个新的 `CompletableFuture`，在所有输入 Future 完成后完成。典型用法如下：

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
List<String> urls = Arrays.asList("url1", "url2", "url3", "url4", "url5");

List<CompletableFuture<String>> futures = urls.stream()
    .map(url -> CompletableFuture.supplyAsync(() -> fetch(url), pool))
    .collect(Collectors.toList());

CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

List<String> results = futures.stream()
    .map(CompletableFuture::join)
    .collect(Collectors.toList());
```

对于简单的"并行跑几个任务、等全部完成"的场景，这段代码足够清晰、够用。不需要引入任何第三方库，JDK 原生支持。

## 做不到什么

### 没有并发控制

上面的代码一次性提交了 5 个任务到线程池。当任务数从 5 变成 5000 时，5000 个 `Callable` 瞬间全部入队。线程池内部的 `LinkedBlockingQueue` 是无界的，队列深度等于 `taskCount - poolSize`。每个排队的 `Callable` 都持有自己的状态（数据引用、HTTP 连接等），内存压力和资源占用与任务总数成正比。详见 [E1. 一次性提交打满队列](E1-queue-flooding.md)。

### 没有超时取消

`allOf().join()` 是无限等待。如果加 `.get(5, SECONDS)`，调用者确实在 5 秒后收到 `TimeoutException`，但线程池里的任务依然在跑，继续占用线程和资源。`allOf` 没有"超时后取消所有任务"的内置机制，你需要手动遍历每个 Future 调用 `cancel(true)`，而且 `cancel` 对不响应中断的任务无能为力。详见 [D1. get(timeout) 后任务还在跑](D1-get-timeout-task-still-running.md)。

### 没有 fail-fast

`allOf` 的语义是"等全部完成"。如果第 1 个任务在 10ms 内就抛了异常，剩余 99 个任务仍然会继续执行到结束，调用者才会在 `join()` 时拿到第一个异常。在生产环境中，当错误已经表明当前批次不可用时，继续跑完剩余任务纯粹是浪费资源。

```java
// allOf 行为：一个失败，其余继续跑
CompletableFuture<String> f1 = CompletableFuture.supplyAsync(() -> {
    throw new RuntimeException("boom");
});
CompletableFuture<String> f2 = CompletableFuture.supplyAsync(() -> {
    Thread.sleep(30_000); // 即使 f1 已经失败，f2 仍然跑满 30 秒
    return "ok";
});
CompletableFuture.allOf(f1, f2).join(); // 30 秒后才抛异常
```

### 没有上下文传播

`CompletableFuture.supplyAsync()` 在 ForkJoinPool 或指定线程池中执行，`ThreadLocal` 上下文不会自动传播。如果你的业务依赖 MDC（日志链路追踪）、用户身份、租户信息等 ThreadLocal 数据，在异步任务中全部丢失。手动传递上下文参数会导致方法签名膨胀，跨层传递时尤其痛苦。详见 [B1. ThreadLocal 跨线程丢失](B1-threadlocal-context-lost.md)。

## 对比表

| 维度 | CompletableFuture.allOf | Par.map() |
|------|------------------------|-----------|
| 并发控制 | 无，一次性全部提交 | 滑动窗口，队列深度 <= parallelism |
| 超时取消 | get 超时后任务继续跑 | 超时自动中断 + 协作式取消 |
| Fail-fast | 无，等全部完成 | 首个失败即取消剩余任务 |
| 上下文传播 | ThreadLocal 丢失 | TTL 自动传播（MDC、身份等） |
| SPI 扩展 | 无 | TaskListener / DeadlockDetectionListener |
| 依赖 | JDK 原生 | Guava + TTL（约 1.5MB） |

## 什么时候用哪个

**用 CompletableFuture.allOf**：任务数量少（< 10）、不需要并发控制、不需要取消、没有跨线程上下文依赖。比如并行调用 3 个独立的微服务聚合数据，allOf 完全够用，引入额外库反而增加复杂度。

**用 Par.map()**：任务数量大或不可控、需要限制并发防止资源耗尽、需要超时后真正取消任务、需要 fail-fast 快速止损、依赖 MDC 等跨线程上下文。典型场景：批量 HTTP 调用、数据库批量查询、文件批量处理等生产级任务。

```java
// parallel-in-scope：并发控制 + 超时取消 + fail-fast + 上下文传播
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

MultiTaskOptions opts = MultiTaskOptions.of("batch-api")
        .parallelism(4)           // 最多 4 个并发
        .timeout(java.time.Duration.ofMillis(5000))            // 5 秒超时
        .taskType(TaskType.IO_BOUND)
        .build();

TaskBatchResult<String> result = par.map( urls, url -> {
    MDC.put("traceId", traceId); // TTL 自动传播到子线程
    return fetch(url);
}, opts);

System.out.println(result.reportString());
// 成功时: SUCCESS:100
// 部分失败时: SUCCESS:95 MEMBER_CANCELED:3 USER_FAILURE:2
// 超时时: 全部取消，线程资源立即释放
```

一句话总结：**CompletableFuture.allOf 是并行等待的"原语"，Par.map() 是生产级批量并行处理的"框架"**。简单场景用原语，生产场景用框架。

---

> 📁 相关 demo：[G5_BatchHttpCallsTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G5_BatchHttpCallsTest.java)
