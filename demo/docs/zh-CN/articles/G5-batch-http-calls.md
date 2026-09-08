# G5. 批量 HTTP 调用——一个失败或批次超时，取消其余未完成任务


## 问题

调用 10 个下游微服务，一个慢了全部等。用 `CompletableFuture.allOf()` 没有并发控制、没有 fail-fast、超时后任务还在跑。典型场景：聚合查询、批量同步、扇出调用。当某个下游服务响应变慢，整个请求链路被拖垮。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
List<String> services = Arrays.asList("order", "user", "payment", "inventory", ...);

// 一次性提交所有任务——没有并发控制
List<CompletableFuture<String>> futures = services.stream()
    .map(svc -> CompletableFuture.supplyAsync(() -> callService(svc), pool))
    .collect(Collectors.toList());

try {
    // 无限等待，一个慢了全部等
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .get(5, TimeUnit.SECONDS);
} catch (TimeoutException e) {
    // 超时了，但线程池里的任务依然在跑，继续占用线程和连接
}
```

三个问题：
1. **无并发控制** — 10 个任务瞬间全部提交，线程池队列堆积
2. **无 fail-fast** — 第 1 个任务 10ms 就报错了，其余 9 个仍然跑满 5 秒
3. **超时后任务继续跑** — `get(timeout)` 只是让调用者超时返回，池中任务不受影响

## 解决方法

`Par.map()` + `MultiTaskOptions` 一行搞定：
- `parallelism(4)` — 最多 4 个并发，滑动窗口调度，队列深度始终受控
- `timeout(3000)` — 本次批量调用最多执行 3 秒，超时后取消所有未完成任务
- fail-fast — 首个任务失败即取消剩余未完成任务

结果通过 `TaskBatchResult` 聚合，`reportString()` 一行查看状态分布。

## 代码

```java
GlobalPar config = GlobalPar.builder()
        .register("http-pool", pool)
        .build();
Par par = config.defaultPar();

MultiTaskOptions opts = MultiTaskOptions.of("batch-http")
        .parallelism(4)           // 最多 4 个并发
        .timeout(java.time.Duration.ofMillis(3000))            // 3 秒超时
        .taskType(TaskType.IO_BOUND)
        .build();

TaskBatchResult<String> result = par.map( services, svc -> {
    return callService(svc);  // 纯业务逻辑
}, opts);

// 等待完成后查看状态
Thread.sleep(3500);
System.out.println(result.reportString());
// 成功时: SUCCESS:10
// 批次超时时可能为: SUCCESS:7,MEMBER_CANCELED:3
// 有异常: SUCCESS:8,USER_FAILURE:1,MEMBER_CANCELED:1
```

对比 `CompletableFuture.allOf()`：同样 10 个任务、同样 3 秒等待上限，`Par.map()` 的批次超时会取消剩余未完成任务；`allOf().get(timeout)` 只让调用者停止等待，不会取消池中的任务。正在执行的 HTTP 调用仍需支持线程中断或其他协作式取消机制，才能及时释放线程和连接。

---

> 完整测试代码：[G5_BatchHttpCallsTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G5_BatchHttpCallsTest.java)
