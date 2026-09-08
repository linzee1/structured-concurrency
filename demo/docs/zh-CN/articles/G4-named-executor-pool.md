# G4. 多个线程池怎么管理——命名注册

## 问题

项目里通常有多个线程池：IO 池处理网络请求、计算池跑 CPU 密集任务、报表池专门生成报表。每个池的线程数、队列策略、拒绝策略都不一样。用原生 Java `ExecutorService` 管理这些池，最大的问题是——池的引用靠变量传递，没有语义区分。

```java
ExecutorService ioPool = Executors.newFixedThreadPool(16);
ExecutorService cpuPool = Executors.newFixedThreadPool(4);
ExecutorService reportPool = Executors.newSingleThreadExecutor();
```

当业务代码需要提交任务时，必须手动把正确的池传进去。一个方法可能需要同时用到两个池，参数列表越来越长；重构时不小心传错了池，编译器不会报错，运行时才发现——IO 密集任务跑到了只有 4 个线程的计算池上，线程瞬间被打满，其他计算任务全部排队。

更麻烦的是线程池的生命周期管理。池散落在各个 Service 类里，`shutdown()` 的时候要逐个找出来关，漏关一个就是线程泄漏。

## 问题复现

```java
ExecutorService ioPool = Executors.newFixedThreadPool(16);
ExecutorService cpuPool = Executors.newFixedThreadPool(4);

// 两个池的引用类型完全一样，传错编译器不报错
processOrders(ioPool);   // 正确：IO 任务用 IO 池
processReport(cpuPool);   // 错误：报表任务误用了计算池
// ↑ 传错了！reportPool 和 cpuPool 都是 ExecutorService，编译通过，运行时才发现线程不够用
```

变量名只是给程序员看的，Java 编译器不关心你叫它 `ioPool` 还是 `cpuPool`。当方法签名是 `void process(ExecutorService pool)` 时，传哪个池都能编译通过，全靠人肉保证正确性。

## 解决方法

`GlobalPar.builder().register("name", pool)` 把每个线程池注册为一个命名实体。提交任务时用 `par.map( ...)` 按名字引用，不再直接传递 `ExecutorService` 实例。

一个 `GlobalPar` 可以注册多个池，统一管理生命周期。名字有业务语义——`"io-pool"`、`"cpu-pool"`、`"report-pool"`——代码自解释，不怕传错。

## 代码

```java
ExecutorService ioPool = Executors.newFixedThreadPool(16);
ExecutorService cpuPool = Executors.newFixedThreadPool(4);
ExecutorService reportPool = Executors.newSingleThreadExecutor();

// 命名注册：一个 GlobalPar 统一管理所有线程池
GlobalPar config = GlobalPar.builder()
        .register("io-pool", ioPool)
        .register("cpu-pool", cpuPool)
        .register("report-pool", reportPool)
        .build();
Par par = config.defaultPar();

// 按名字引用，不可能传错
MultiTaskOptions ioOpts = MultiTaskOptions.of("fetch-orders").taskType(TaskType.IO_BOUND).parallelism(8).timeout(java.time.Duration.ofMillis(5000)).build();
TaskBatchResult<Order> orders = par.map( orderIds, id -> {
    return orderService.fetch(id);  // IO 任务走 io-pool
}, ioOpts);

MultiTaskOptions cpuOpts = MultiTaskOptions.of("compute-score").taskType(TaskType.CPU_BOUND).parallelism(4).timeout(java.time.Duration.ofMillis(10000)).build();
TaskBatchResult<Double> scores = par.map( users, user -> {
    return scoreEngine.compute(user);  // 计算任务走 cpu-pool
}, cpuOpts);

MultiTaskOptions reportOpts = MultiTaskOptions.of("gen-report").parallelism(1).timeout(java.time.Duration.ofMillis(60000)).build();
TaskBatchResult<Report> reports = par.map( months, month -> {
    return reportService.generate(month);  // 报表任务走 report-pool
}, reportOpts);
```

对比原生方式：

| 维度 | 原生 ExecutorService 传递 | GlobalPar 命名注册 |
|------|-------------------------|------------------|
| 池引用 | 方法参数，类型相同易混 | 按名字引用，语义明确 |
| 传错风险 | 编译通过，运行时才发现 | 名字不匹配直接报错 |
| 生命周期管理 | 散落各处，容易漏关 | 集中在 GlobalPar，统一管理 |
| 新增池 | 改方法签名，改调用方 | `.register("new-pool", pool)` 一行注册 |

---

> 📁 完整测试代码：[G4_NamedExecutorPoolTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G4_NamedExecutorPoolTest.java)
