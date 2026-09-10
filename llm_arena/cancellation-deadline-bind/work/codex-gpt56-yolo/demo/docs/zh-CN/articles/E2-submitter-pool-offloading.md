# E2. 提交循环占用业务线程

## 问题

（接上篇餐厅比喻：大堂经理负责"安排下一桌坐下"，但经理应该在哪里办公？）

在实现滑动窗口并发控制时，一个常见的做法是：先提交一批任务（数量等于并发度），然后在循环中等待任意一个任务完成，再提交下一个任务。这个"等待完成 → 提交下一个"的循环就是提交循环。

问题在于，如果将这个提交循环也提交到业务线程池中运行，它就会永久占用一个业务线程。提交循环的核心是调用 `ExecutorCompletionService.take()` 阻塞等待任务完成——这意味着在等待期间，该线程不做任何实际工作，只是空等。对于一个固定大小的线程池来说，每多一个提交循环在运行，就少一个线程执行真正的业务逻辑。

最极端的情况：当线程池大小等于并发度时，提交循环占用一个线程后，剩余线程数不足以支撑初始并发度，甚至可能引发死锁——提交循环等待任务完成，但任务无法获得线程来执行。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(1);
ExecutorCompletionService<Integer> ecs = new ExecutorCompletionService<>(pool);

// 提交第一个任务
ecs.submit(() -> { Thread.sleep(100); return 1; });

// 将提交循环也提交到同一个线程池（朴素实现）
pool.submit(() -> {
    for (int i = 1; i < 3; i++) {
        ecs.take();       // 阻塞等待任务完成
        ecs.submit(() -> { Thread.sleep(100); return 1; });
    }
});

// 结果：死锁！
// task0 完成后，提交循环在唯一线程上运行，
// 调用 take() 等待 task1，但 task1 无法获得线程执行
```

## 解决方法

`parallel-in-scope` 使用专用的 `SubmitterPool` 来运行提交循环。这是一个全局单例的守护线程池，线程命名为 `Par-Submitter-*`，采用 `CachedThreadPool` 模型按需创建。

提交循环在 `SubmitterPool` 上运行，只负责"等待完成 → 提交下一个"的调度逻辑，不执行任何业务代码。业务线程池的线程始终只执行实际的业务任务，不会被调度开销占用。

这种分离带来两个关键好处：
- **避免死锁**：即使业务线程池大小等于并发度，也不会因为提交循环占用线程而导致死锁
- **最大化吞吐**：业务线程池的所有线程都用于执行任务，有效并行度等于线程池大小

## 代码

```java

// 仅 1 个线程的业务线程池
ExecutorService pool = Executors.newFixedThreadPool(1);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

BatchExecutionOptions opts = BatchExecutionOptions.of("offload-demo")
        .parallelism(1)
        .build();

// Par.map() 不会死锁——提交循环运行在 Par-Submitter 线程上
List<Integer> input = Arrays.asList(1, 2, 3);
AsyncBatchResult<Integer> result = par.map( input, x -> {
    Thread.sleep(100);
    return x * 2;
}, opts);

// 所有任务正常完成：[2, 4, 6]
```

---

> 📁 完整测试代码：[E2_SubmitterPoolOffloadingTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/E2_SubmitterPoolOffloadingTest.java)
