# 批量调用最佳实践：HTTP/DB/RPC 完整方案


批量调用是后端开发中最常见的并发场景。本文通过三个真实场景，展示如何用 `parallel-in-scope` 一行代码解决并发控制、超时、fail-fast 问题。

---

## 场景一：批量 HTTP 调用

调用 10 个下游微服务（订单、用户、支付、库存...），要求：
- 最多 5 个并发（别把下游打爆）
- 3 秒超时（快速失败）
- 一个挂了就取消其余（fail-fast）

```java
GlobalPar config = GlobalPar.builder()
        .register("http-pool", Executors.newFixedThreadPool(8))
        .build();
Par par = config.defaultPar();

List<String> services = Arrays.asList(
        "order", "user", "payment", "inventory", "notification",
        "billing", "shipping", "review", "recommendation", "analytics");

BatchExecutionOptions opts = BatchExecutionOptions.of("batch-http").taskType(TaskType.IO_BOUND)
        .parallelism(5)        // 最多 5 个并发，保护下游
        .timeout(java.time.Duration.ofMillis(3000))         // 3 秒超时
        .build();

AsyncBatchResult<String> result = par.map( services, svc -> {
    return callDownstream(svc);  // 你的 HTTP 调用逻辑
}, opts);

Thread.sleep(3500);  // 等待任务完成
System.out.println(result.reportString());
// 正常: SUCCESS:10
// 部分超时: SUCCESS:7,CANCELLED:3
// 有异常: SUCCESS:8,FAILED:1,CANCELLED:1
```

关键点：
- `taskType(IO_BOUND)` 让框架知道这是 IO 任务，线程调度策略自动优化
- `parallelism(5)` 用滑动窗口调度，一个完成才提交下一个，队列深度始终受控
- fail-fast 自动生效——首个任务抛异常，剩余未完成任务立即被取消

---

## 场景二：数据库分片查询

查询 10000 条用户数据，按 ID 分 10 片并行查询，最后合并结果：

```java
List<Long> allIds = IntStream.rangeClosed(1, 10000)
        .mapToObj(Long::valueOf).collect(Collectors.toList());

// 手动分片：每 1000 个 ID 一片
List<List<Long>> shards = new ArrayList<>();
for (int i = 0; i < allIds.size(); i += 1000) {
    shards.add(allIds.subList(i, Math.min(i + 1000, allIds.size())));
}
// shards.size() == 10

BatchExecutionOptions opts = BatchExecutionOptions.of("db-batch-query").taskType(TaskType.IO_BOUND)
        .parallelism(3)        // DB 连接池就 3 个，别超了
        .timeout(java.time.Duration.ofMillis(30000))        // 查询可能慢，30 秒超时
        .build();

AsyncBatchResult<List<User>> result = par.map( shards, shard -> {
    return userMapper.selectByIds(shard);  // 你的 DAO 调用
}, opts);

// 合并分片结果
List<User> allUsers = new ArrayList<>();
for (ListenableFuture<List<User>> future : result.getResults()) {
    allUsers.addAll(future.get(30, TimeUnit.SECONDS));
}
System.out.println("查询到 " + allUsers.size() + " 条记录");
```

关键点：
- `parallelism(3)` 匹配 DB 连接池大小，不会超出连接数
- 分片数 > parallelism 时，框架自动排队——先跑 3 片，完成一个再启动下一个
- 30 秒超时覆盖慢查询场景，超时后自动取消剩余分片

---

## 场景三：混合 IO 调用

一个请求需要同时调 HTTP、查 DB、读缓存，三种 IO 混在一个批次里：

```java
BatchExecutionOptions opts = BatchExecutionOptions.of("mixed-io").taskType(TaskType.IO_BOUND)
        .parallelism(6)
        .timeout(java.time.Duration.ofMillis(5000))         // 统一 5 秒超时
        .build();

AsyncBatchResult<Object> result = par.map( tasks, task -> {
    if (task instanceof HttpRequest) {
        return httpClient.execute((HttpRequest) task);   // HTTP 调用
    } else if (task instanceof DbQuery) {
        return jdbcTemplate.query((DbQuery) task);        // DB 查询
    } else {
        return cacheClient.get((CacheKey) task);          // 缓存读取
    }
}, opts);

// fail-fast: 任何一个失败，其余自动取消
Thread.sleep(5500);
String report = result.reportString();
```

关于超时：用统一的 `BatchExecutionOptions.timeout` 即可，不需要每个任务设不同超时。原因：
- 框架级超时是"兜底"，防止任务永远挂起
- 如果某个调用需要更细粒度的超时，在任务内部自己处理（比如 HTTP client 的 connectTimeout/readTimeout）
- 这样保持 `BatchExecutionOptions` 简洁，任务逻辑自包含

---

## 通用模式

### 1. 超时必须设

不设超时 = 任务可能永远挂起。框架默认 60 秒超时，但建议根据业务场景显式设置：

```java
// 快速 HTTP 调用
BatchExecutionOptions.of("http").taskType(TaskType.IO_BOUND).timeout(java.time.Duration.ofMillis(3000)).build();

// 数据库查询
BatchExecutionOptions.of("db").taskType(TaskType.IO_BOUND).timeout(java.time.Duration.ofMillis(30000)).build();

// 文件处理
BatchExecutionOptions.of("file").taskType(TaskType.IO_BOUND).timeout(java.time.Duration.ofMillis(120000)).build();
```

### 2. 并行度要匹配资源

`parallelism` 不是越大越好，要匹配下游资源：

| 场景 | 推荐 parallelism | 原因 |
|------|-------------------|------|
| HTTP 调用 | 5-10 | 别把下游打爆 |
| DB 查询 | = 连接池大小 | 超了就排队等连接 |
| 缓存读取 | 10-20 | 缓存通常能扛更高并发 |
| 文件 IO | 3-5 | 磁盘 IO 是瓶颈 |

### 3. 用 report() 快速判断

```java
// 一行看全貌
String report = result.reportString();
// "SUCCESS:8,FAILED:1,CANCELLED:1 | firstException=timeout"

// 结构化访问
BatchReport r = result.report();
Map<FutureState, Integer> counts = r.getStateCounts();
Throwable firstError = r.getFirstException();
```

生产环境中，可以把 `reportString()` 打到日志里，配合 TaskListener 做监控告警。

### 4. 异常不要吞

在 lambda 里 catch 异常返回 null 是最常见的坑：

```java
// 错误: 吞掉了异常，report 永远显示 SUCCESS:10
par.map( items, item -> {
    try {
        return riskyCall(item);
    } catch (Exception e) {
        return null;  // 隐藏了失败！
    }
}, opts);
```

让异常自然抛出，框架会自动记录到 `report()` 里，fail-fast 也会正确触发。

---

## 反模式

**1. `parallelism=Integer.MAX_VALUE`**

```java
// 错误: 等于没有并发控制
BatchExecutionOptions.of("bad").parallelism(Integer.MAX_VALUE).build();
```

正确做法：设一个合理的值，匹配下游资源。

**2. 不设超时**

```java
// 错误: 任务可能永远挂起
BatchExecutionOptions.of("bad").build();  // timeout=0，依赖默认 60 秒
```

正确做法：根据场景显式设超时。

**3. 在 lambda 里 catch 异常返回 null**

```java
// 错误: 隐藏失败，report 永远是 SUCCESS
par.map( items, item -> {
    try {
        return call(item);
    } catch (Exception e) {
        log.error("failed", e);
        return null;  // 框架认为这是成功!
    }
}, opts);
```

正确做法：让异常抛出，用 `reportString()` 统一处理。

---

> 完整测试代码：[BatchBestPracticesTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/BatchBestPracticesTest.java)
