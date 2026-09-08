# I3. 滑动窗口 vs 信号量——并发控制的两种思路


在需要限制并发数量的场景中，信号量（`Semaphore`）和滑动窗口（Sliding Window）是两种常见的方案。它们目标相同——控制同时执行的任务数，但实现路径和实际效果有本质区别。

## 信号量方案

信号量是最直观的做法：用一个 `Semaphore` 包装每个任务，许可数等于期望的并行度。所有任务一次性提交，获取许可后才真正执行：

```java
Semaphore sem = new Semaphore(parallelism);
for (Item item : items) {
    pool.submit(() -> {
        sem.acquire();
        try {
            return process(item);
        } finally {
            sem.release();
        }
    });
}
```

代码简洁，逻辑清晰。但问题在于：**所有任务在第一行 `pool.submit()` 就全部进入了线程池队列**。如果 `items` 有 1000 个，线程池队列瞬间堆积 1000 个任务对象。虽然只有 `parallelism` 个任务在真正执行，其余 998 个都在队列里空等信号量许可——它们持有数据引用、占用堆内存，却什么都不做。

## 滑动窗口方案

滑动窗口换了一个思路：**不是全部提交再排队，而是有空位才提交新任务**。初始只提交 `parallelism` 个任务，每当有一个任务完成，再提交下一个：

```java
// 伪代码：滑动窗口核心逻辑
int start = Math.min(tasks.size(), parallelism);

// 第一批：提交 parallelism 个任务
for (int i = 0; i < start; i++) {
    completionService.submit(tasks.get(i));
}

// 剩余任务：完成一个，提交一个
while (index < tasks.size()) {
    completionService.take(); // 阻塞等待下一个完成
    completionService.submit(tasks.get(index++));
}
```

`parallel-in-scope` 的 `SlidingWindowSubmitter` 就是这个模式的实现。它用内部 `ListenableCompletionService` 监听完成事件，`blockingQueue.take()` 作为信号驱动后续提交。

## 关键区别

### 任务提交时机

| 维度 | 信号量 | 滑动窗口 |
|------|--------|----------|
| 提交时机 | 立即提交全部任务 | 只提交 parallelism 个，后续按需提交 |
| 队列深度 | 等于任务总数 | 始终 <= parallelism |

信号量方案中，任务的生命周期是"提交 -> 队列等待 -> 获取许可 -> 执行 -> 释放"。滑动窗口中，任务的生命周期是"轮到它时才提交 -> 直接执行"。等待发生在提交之前，而非之后。

### 线程池压力

信号量方案把等待压力从任务本身转移到了线程池队列。对于 `FixedThreadPool`（默认无界队列），1000 个任务意味着队列里有 ~998 个等待项。每个等待项都是一个 `FutureTask` 对象，持有 `Callable` 引用，而 `Callable` 又持有业务数据。这些对象在队列中长时间驻留，直到被执行才释放。

滑动窗口中队列深度恒定，内存占用与并行度成正比，而非与任务总数成正比。处理 1000 个任务和处理 10 个任务，线程池的压力是一样的。

### 取消传播

这是最容易被忽略的差异。信号量方案中，取消一个任务需要：

```java
Future<?> f = pool.submit(() -> {
    sem.acquire();
    try { ... } finally { sem.release(); }
});
f.cancel(true); // 只取消了这一个，其他 999 个还在队列里
```

要取消全部任务，需要额外维护一个 `volatile boolean cancelled` 标志，每个任务在 `acquire()` 前检查。即便如此，已提交到队列的任务仍然占着内存，直到线程池调度到它们时才会退出。

滑动窗口天然支持批量取消：取消提交循环即停止后续任务的提交，已完成和正在执行的任务不受影响，未提交的任务根本不会进入线程池。`parallel-in-scope` 的 `CancellationToken` 正是基于这个机制实现的级联取消。

## 为什么选滑动窗口

`parallel-in-scope` 选择滑动窗口而非信号量，核心原因有三个：

1. **队列可控**：队列深度有上界，不会因任务数量暴涨而 OOM。对于批量处理场景（如数据库导出、API 批量调用），这是刚需。

2. **取消干净**：取消操作只需中断提交循环，未提交的任务天然不会执行。配合 `CancellationToken` 的 late-bind 机制，可以实现超时取消、失败快速取消、父子级联取消。

3. **资源隔离**：`SlidingWindowSubmitter` 使用独立的 `submitterPool` 运行提交循环，不占用工作线程池的线程。信号量方案没有这种隔离——等待许可的线程虽然阻塞，但仍然占着工作池的一个线程槽位。

信号量方案胜在简单，适合任务数量可控、不需要取消的轻量场景。但当任务量大、需要生命周期管理时，滑动窗口是更稳健的选择。

---

> 📁 完整测试代码：[E1_QueueFloodingTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/E1_QueueFloodingTest.java)
