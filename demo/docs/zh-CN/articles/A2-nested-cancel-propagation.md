# A2. 嵌套任务取消不到


## 问题

在实际业务中，嵌套并行是常见模式：外层任务批量处理订单，每个订单内部又需要并行调用多个下游服务。当外层任务超时或需要取消时，开发者期望所有相关工作都能立即停止。然而使用原生 `ExecutorService` 时，取消信号无法穿透到内层任务。

根本原因在于 Java 的 `Future.cancel(true)` 只会中断执行当前任务的线程。外层任务虽然被中断了，但它在执行过程中提交给线程池的内层任务运行在各自独立的线程上，完全不受影响。这些内层任务会继续消耗线程池资源直到自然完成，造成资源浪费，甚至在高并发场景下引发线程池耗尽。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(8);

// 外层任务
Future<?> outer = pool.submit(() -> {
    // 内层任务在独立线程上运行
    Future<?> inner = pool.submit(() -> {
        Thread.sleep(5000); // 模拟长时间下游调用
        return "inner-result";
    });
    inner.get(); // 等待内层完成
});

outer.cancel(true); // 取消外层任务
// 问题：内层任务仍在运行，不会被中断！
```

调用 `outer.cancel(true)` 后，外层线程被中断并退出，但内层任务的线程毫不知情，继续执行 5 秒才结束。如果每个外层任务都产生多个内层任务，这些"孤儿"任务会迅速耗尽线程池。

## 解决方法

`Par.map()` 内部维护了父子 `CancellationToken` 链来解决这个问题。框架在执行链路上下传递 token 引用：外层 `Par.map()` 创建的 `CancellationToken` 作为父 token，内层 `Par.map()` 自动从当前线程上下文读取父 token 并创建子 token。

当外层超时触发取消时，取消信号沿 token 链向下传播，所有子 token 都会被标记为已取消。子任务在执行过程中通过 `Thread.sleep()`、`Future.get()` 等阻塞操作响应中断，从而实现真正的嵌套取消。

关键机制：
- **父子链**：内层 `CancellationToken` 自动挂载到外层 token，无需手动管理
- **超时传播**：外层超时触发的取消会级联到所有子任务
- **中断传递**：子任务的线程被中断，阻塞操作抛出 `InterruptedException`

## 代码

```java
GlobalPar config = GlobalPar.builder()
        .register("pool", Executors.newFixedThreadPool(8))
        .build();
Par par = config.defaultPar();

// 外层配置：500ms 超时
MultiTaskOptions outerOptions = MultiTaskOptions.of("outer")
        .timeout(java.time.Duration.ofMillis(500))
        .build();

List<String> orders = Arrays.asList("ORD-001", "ORD-002", "ORD-003");

TaskBatchResult<String> result = par.map( orders, order -> {
    // 内层并行调用多个下游服务
    MultiTaskOptions innerOptions = MultiTaskOptions.of("inner")
            .parallelism(3)
            .inheritTimeout()  // 嵌套任务：继承外层 deadline
            .build();
    List<String> services = Arrays.asList("inventory", "payment", "shipping");

    TaskBatchResult<String> innerResult =
            par.map( services, svc -> callDownstream(svc, order), innerOptions);

    // 等待内层结果
    return aggregate(innerResult);
}, outerOptions);

// 当外层超时后，所有内层任务自动取消，线程资源立即释放
```

只需将嵌套的并行调用都使用 `Par.map()`，取消传播由框架自动处理，开发者无需手动管理 `CancellationToken` 的生命周期。

---

> 📁 完整测试代码：[A2_NestedCancelPropagationTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/A2_NestedCancelPropagationTest.java)
