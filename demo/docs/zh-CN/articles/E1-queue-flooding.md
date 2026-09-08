# E1. 一次性提交打满队列

## 问题

想象一个场景：一家餐厅只有 4 张桌，门口来了 100 桌客人。如果大堂经理把 100 桌全放进餐厅站着等——过道堵满，服务员挤不过去，整个餐厅瘫痪。

Java 并发编程中同样的事情每天都在发生。当需要并行处理大量任务时，开发者通常的做法是把所有任务一次性提交到线程池：

```java
for (Item item : items) {
    pool.submit(() -> process(item));
}
```

`FixedThreadPool` 内部使用无界的 `LinkedBlockingQueue`，所有任务立即入队。100 个任务提交到 2 线程的池子，队列瞬间堆积 98 个待执行项。每个 `Callable` 持有自己的状态——可能是大量数据、数据库连接、HTTP 客户端引用。队列深度达到数百时，这些"排队等待"的对象长时间驻留堆内存，造成 GC 频繁暂停，极端情况触发 OOM。

更隐蔽的问题是：如果任务持有稀缺资源（如数据库连接），大量排队任务会在真正执行之前就占满连接池，导致运行中的任务反而拿不到连接。

## 问题复现

```java
int poolSize = 2;
int taskCount = 100;
ExecutorService pool = Executors.newFixedThreadPool(poolSize);

// 一次性提交 100 个任务——餐厅瞬间挤满人
for (int i = 0; i < taskCount; i++) {
    pool.submit(() -> {
        Thread.sleep(1000);
        return process(data);
    });
}

// ((ThreadPoolExecutor) pool).getQueue().size() → ~98
```

## 解决方法

`parallel-in-scope` 的 `Par.map()` 采用**滑动窗口调度**，就像一个靠谱的大堂经理：

**大堂经理只安排前 4 桌入座。第 1 桌吃完走了，经理立刻安排第 5 桌坐下；第 2 桌走了，安排第 6 桌……始终保持 4 桮满座，门口有人等但餐厅内部不堵。**

技术上：`SlidingWindowSubmitter` 初始只提交 `parallelism` 个任务，每当内部 completion queue 收到一个完成事件，才提交下一个。队列深度永远不超过并行度。

两个关键细节：

1. **经理不在大堂办公。** "安排下一桌"这个调度动作，经理在独立的办公室完成，不在大堂占桌子。——滑动窗口调度循环跑在独立的 `Par-Submitter-*` 守护线程上，不占业务线程池的线程。

2. **限时最后才宣布。** 100 桌客人全部到齐后，经理才统一宣布"就餐限时 1 小时"，而不是第 1 桌坐下就开始计时。——所有任务提交完毕后，再统一绑定超时和 fail-fast（lateBind 机制）。

## 代码

```java

// 餐厅只有 4 张桌（4 线程）
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 并行度 2：一轮最多 2 桌同时就餐，队列深度最多 2
MultiTaskOptions options = MultiTaskOptions.of("data-process")
        .parallelism(2)
        .timeout(java.time.Duration.ofMillis(30000))
        .build();

// 100 个任务——但队列深度始终 <= 2，餐厅内部不堵
List<Data> items = loadLargeDataset();
TaskBatchResult<Result> result = par.map( items, item -> {
    return process(item);
}, options);

System.out.println(result.reportString());
// 输出: SUCCESS:100
```

对比两种方式：

| 维度 | 直接 submit（全放进餐厅） | Par.map() 滑动窗口（经理安排） |
|------|------------|-------------------|
| 队列深度 | taskCount - poolSize | <= parallelism |
| 内存占用 | 与任务总数线性增长 | 与并行度线性增长 |
| 资源占用 | 排队时就占用资源 | 按需占用，用完释放 |
| 背压控制 | 无 | 内置 |

---

> 📁 完整测试代码：[E1_QueueFloodingTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/E1_QueueFloodingTest.java)
