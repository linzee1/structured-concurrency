# B1. ThreadLocal 提交到线程池后消失

## 问题

在 Java Web 应用中，`MDC`（Mapped Diagnostic Context）是日志链路追踪的标准做法。请求进入时，拦截器将 `traceId` 写入 `MDC`，后续所有日志自动携带该 ID，方便在 ELK 等日志平台中串联一次请求的完整调用链。然而，`MDC` 底层基于 `ThreadLocal`，而 `ThreadLocal` 的值是线程隔离的——当你把任务提交到线程池时，工作线程并没有主线程的 `MDC` 上下文。

这意味着：主线程打了 `"traceId=abc-123 ..."` 的日志，而线程池中的任务打出的日志却缺少 `traceId`，变成孤立日志。在高并发场景下，一旦某个并行任务抛出异常，你无法通过 `traceId` 回溯到底是哪次请求触发的。日志链路在这里断掉了，排查问题变成大海捞针。

同样的问题也存在于 `userId`、`orgId`、`requestId` 等任何基于 `ThreadLocal` 的请求级上下文。这不是 MDC 的 bug，而是 `ThreadLocal` 的本质限制：它只能在当前线程访问，无法自动跨越线程边界。

## 问题复现

```java
// 主线程设置 MDC
MDC.put("traceId", "abc-123");

ExecutorService pool = Executors.newFixedThreadPool(4);
List<Future<String>> futures = new ArrayList<>();
for (int i = 0; i < 3; i++) {
    futures.add(pool.submit(() -> {
        // 工作线程读取 MDC —— 返回 null，上下文丢失
        String traceId = MDC.get("traceId");
        log.info("traceId={}, 处理任务", traceId);  // traceId=null!
        return "done";
    }));
}
// 结果：所有工作线程的 traceId 都是 null
```

## 解决方法

`parallel-in-scope` 会在 `Par.map()` 边界捕获 `TransmittableThreadLocal`，并在每个任务执行前回放、执行后恢复。普通 `ThreadLocal` 不会传播；MDC 只有采用 TTL 兼容适配器时才会随任务传播。

任务包装器在完成后释放捕获快照，避免已结束任务继续持有请求上下文。框架自身的取消、deadline 和嵌套关系仍由显式的任务上下文管理，不依赖 TTL。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("biz-pool", pool)
        .build();
Par par = config.defaultPar();

// 主线程设置 MDC（需要 TTL 兼容的 MDC 适配器）
MDC.put("traceId", "abc-123");

// 配置并行选项
MultiTaskOptions opts = MultiTaskOptions.of("process-orders")
        .parallelism(4)
        .timeout(java.time.Duration.ofMillis(5000))
        .build();

// 并行处理订单
List<Order> orders = orderRepository.findPending();
TaskBatchResult<ProcessResult> result = par.map( orders, order -> {
    // TTL 兼容的 MDC 中可以读取 traceId
    log.info("处理订单: {}", order.getId());
    return processOrder(order);
}, opts);

// 批次取消与超时由任务上下文管理；MDC 通过 TTL 快照传播
System.out.println(result.reportString());
```

---

> 📁 完整测试代码：[B1_MdcContextLostTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/B1_MdcContextLostTest.java)
