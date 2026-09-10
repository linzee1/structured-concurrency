# B2. 函数签名膨胀


## 问题

在微服务架构中，一次业务调用往往需要透传大量基础设施上下文：`traceId` 用于链路追踪，`userId` 和 `orgId` 用于权限校验，`timeout` 用于超时控制，`cancellationToken` 用于取消传播。当这些参数需要传递给并行任务时，开发者不得不将它们塞进每一个业务函数的签名中。

原本简洁的 `fetch(String url)` 变成了 `fetch(String url, String traceId, String userId, String orgId, long timeout, CancellationToken token, RetryPolicy retryPolicy)` —— 7 个参数中只有 1 个是业务参数，其余 6 个全是基础设施"管道"。这不仅让代码可读性急剧下降，还导致每新增一个上下文字段就要修改所有函数签名，牵一发而动全身。更严重的是，这种膨胀会蔓延到整条调用链：Service 层传给 DAO 层，DAO 层传给工具类，层层透传，代码变成参数的搬运工。

## 问题复现

```java
// 原本的业务函数签名，简洁明了
String fetch(String url) { ... }

// 为了支持并行上下文传递，被迫膨胀
String fetch(String url, String traceId, String userId, String orgId,
             long timeout, CancellationToken token, RetryPolicy retry) {
    // 手动检查取消状态
    token.checkCancellation();
    // 手动设置 traceId 到 MDC
    MDC.put("traceId", traceId);
    try {
        return doFetch(url, timeout, retry);
    } finally {
        MDC.remove("traceId");
    }
}

// 调用方也需要搬运所有参数
List<String> results = urls.parallelStream()
    .map(url -> fetch(url, traceId, userId, orgId, timeout, token, retry))
    .collect(Collectors.toList());
```

## 解决方法

`parallel-in-scope` 为 `Par.map()` 任务显式管理取消、deadline、并发限制和 SPI 回调，因此业务 lambda 不需要接收这些框架状态。应用自己的 trace、用户和租户信息可以显式传参；若它们存放在 `TransmittableThreadLocal` 中，也会在 `Par.map()` 边界被捕获并传播。

使用 `Par.map()` 时，函数签名只保留业务参数。需要在任务中响应取消时调用 `Checkpoints`，框架会从当前任务上下文读取对应的取消令牌。

## 代码

```java

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("http-pool", pool)
        .build();
Par par = config.defaultPar();

// 并行选项：框架自动管理超时和取消
BatchExecutionOptions opts = BatchExecutionOptions.of("fetch-data")
        .parallelism(5)
        .timeout(java.time.Duration.ofMillis(3000))
        .build();

// 函数签名只保留业务参数，零基础设施噪音
List<String> urls = Arrays.asList("url1", "url2", "url3", "url4", "url5");
AsyncBatchResult<String> result = par.map( urls, url -> {
    // 框架已自动处理：
    //   - CancellationToken 取消检查（ScopedCallable 内部）
    //   - 超时控制（CancellationToken.lateBind）
    //   - 并发限制（ConcurrentLimitExecutor 滑动窗口）
    //   - SPI 回调（TaskListener）
    // 只需关注业务逻辑
    return doFetch(url);
}, opts);

// 获取结果
System.out.println(result.reportString());
```

---

> 📁 完整测试代码：[B2_FunctionSignatureBloatTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/B2_FunctionSignatureBloatTest.java)
