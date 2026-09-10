# H1. null 和空列表——防御性处理

## 问题

在 Java 并发编程中，批量并行处理是常见模式：从数据库查出一批用户 ID，逐个调用远程接口，汇总结果。但如果传入的列表是 `null` 或空集合，标准的 `ExecutorService` 不会帮你处理——直接 NPE 或者白白提交一轮空任务，浪费资源。

开发者被迫在每个调用点手动加防御检查：

```java
if (ids == null || ids.isEmpty()) {
    return Collections.emptyList();
}
```

这段代码看起来简单，但它散落在项目各处，重复且容易遗漏。漏写一个就是生产事故——NPE 导致接口 500，或者空列表触发无意义的线程池提交，白白消耗线程上下文切换开销。

更隐蔽的问题是：当上游逻辑返回 `null`（比如 MyBatis 查询无结果返回 `null` 而非空列表），下游的并行处理代码如果没有逐层防御，就会在运行时炸掉。这类 bug 在单元测试中很难覆盖到，因为测试数据通常是正常非空的。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);

// 场景 1：null 列表 → NPE
List<String> nullList = null;
for (String item : nullList) {          // NullPointerException
    pool.submit(() -> process(item));
}

// 场景 2：空列表 → 白提交任务
List<String> emptyList = Collections.emptyList();
List<Future<?>> futures = new ArrayList<>();
for (String item : emptyList) {
    futures.add(pool.submit(() -> process(item)));
}
// futures 为空，但上面的 for 循环和列表创建本身已经是无意义开销
// 如果这段逻辑在高 QPS 接口中，每次空轮询都在消耗 CPU
```

## 解决方法

`parallel-in-scope` 的 `Par.map()` 内置了防御性处理。在执行并行逻辑之前，框架会检查输入列表：

- **`null` 列表**：直接返回空的 `AsyncBatchResult`，不抛异常，不提交任何任务
- **空列表**：同样返回空的 `AsyncBatchResult`，零开销

这意味着调用方无需任何防御代码，直接把结果交给下游即可。`AsyncBatchResult.getResults()` 返回空列表，`report()` 返回空的状态统计，不会出现 NPE。

这种设计遵循"库应该宽容地接受输入"的原则：框架替你兜底，让业务代码专注于核心逻辑。

## 代码

```java

// 配置 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

BatchExecutionOptions opts = BatchExecutionOptions.of("user-query").build();

// 无需防御，null 列表安全返回
List<String> nullList = null;
AsyncBatchResult<String> r1 = par.map( nullList, id -> queryUser(id), opts);
assert r1.getResults().isEmpty();  // true

// 空列表同样安全
List<String> emptyList = Collections.emptyList();
AsyncBatchResult<String> r2 = par.map( emptyList, id -> queryUser(id), opts);
assert r2.getResults().isEmpty();  // true

// 正常列表不受影响
List<String> ids = Arrays.asList("user-1", "user-2", "user-3");
AsyncBatchResult<String> r3 = par.map( ids, id -> queryUser(id), opts);
assert r3.getResults().size() == 3;  // true
```

---

> 📁 完整测试代码：[H1_NullEmptyInputTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/H1_NullEmptyInputTest.java)
