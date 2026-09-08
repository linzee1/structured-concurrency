# G6. 数据库批量查询——分片并行

## 问题

数据报表、导出、对账等业务场景经常需要从数据库批量读取大量数据。当数据量达到数万条时，单次 `SELECT` 查询耗时可能长达数秒甚至数十秒，不仅拖慢接口响应，还可能因为长事务持有数据库连接而影响整体吞吐。

常见做法是把 ID 列表分成若干片（shard），每片并行查询，最后合并结果。但手动实现分片并行需要处理很多细节：手动创建线程池、手动分片、`CountDownLatch` 等待所有分片完成、遍历 `Future` 收集结果、异常处理、超时控制……代码量动辄上百行，且容易遗漏边界情况。

```java
// 手动分片并行——代码量大、易出错
int shardSize = 1000;
List<List<Long>> shards = partition(allIds, shardSize);
ExecutorService pool = Executors.newFixedThreadPool(10);
List<Future<List<User>>> futures = new ArrayList<>();
for (List<Long> shard : shards) {
    futures.add(pool.submit(() -> userDao.selectByIds(shard)));
}
List<User> allUsers = new ArrayList<>();
for (Future<List<User>> f : futures) {
    allUsers.addAll(f.get(30, TimeUnit.SECONDS)); // 超时？异常？全靠手写
}
```

## 问题复现

上面的代码存在几个隐患：

1. **线程池生命周期**：每次调用都 `newFixedThreadPool`，调用完忘了 `shutdown` 就会线程泄漏
2. **超时处理粗糙**：`f.get(timeout)` 超时后，其余 `Future` 仍在执行，资源未释放
3. **异常传播困难**：某个分片查询失败后，需要手动决定是继续还是中断
4. **并发度不可控**：10 个分片全部同时打到数据库，可能超过数据库连接池上限

## 解决方法

`parallel-in-scope` 的 `Par.map()` 天然适合"分片并行"场景。只需三步：

1. 把 ID 列表按片大小切分成 `List<List<Long>>` 分片列表
2. 调用 `par.map( shards, shard -> userDao.selectByIds(shard), options)`
3. 从 `TaskBatchResult` 中收集每个分片的结果

`Par.map()` 的滑动窗口调度确保并发度受控（通过 `parallelism` 参数），不会一次性打满数据库连接池。内置超时和异常处理让代码从上百行缩减到十几行。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.GlobalPar;

// 1. 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(8);
GlobalPar config = GlobalPar.builder()
        .register("db-pool", pool)
        .build();
Par par = config.defaultPar();

// 2. 构建分片：把 10000 个 ID 切成 10 片，每片 1000 个
List<Long> allIds = loadAllIds(); // 10000 个
int shardSize = 1000;
List<List<Long>> shards = new ArrayList<>();
for (int i = 0; i < allIds.size(); i += shardSize) {
    shards.add(allIds.subList(i, Math.min(i + shardSize, allIds.size())));
}

// 3. 并行查询，parallelism=5 表示最多同时 5 个分片在执行
MultiTaskOptions options = MultiTaskOptions.of("db-batch-query").taskType(TaskType.IO_BOUND)
        .parallelism(5)
        .timeout(java.time.Duration.ofMillis(30000))
        .build();

TaskBatchResult<List<User>> result = par.map( shards, shard -> {
    return userDao.selectByIds(shard);
}, options);

// 4. 收集所有分片的结果
List<User> allUsers = new ArrayList<>();
for (ListenableFuture<List<User>> future : result.results()) {
    allUsers.addAll(future.get());
}

System.out.println("查询完成: " + allUsers.size() + " 条");
// 查询完成: 10000 条
```

对比两种方式：

| 维度 | 手动分片并行 | Par.map() 分片并行 |
|------|------------|-------------------|
| 代码量 | 50+ 行（池管理 + 分片 + 收集 + 异常） | ~10 行核心逻辑 |
| 并发控制 | 全部分片同时执行 | parallelism 精确控制 |
| 超时处理 | 手动逐个 Future.get(timeout) | 内置统一超时 |
| 异常处理 | 手动 try-catch + 传播 | 自动捕获，reportString() 汇总 |
| 资源释放 | 需手动 shutdown | 由调用方管理（可复用池） |

---

> 📁 完整测试代码：[G6_BatchDbQueryTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G6_BatchDbQueryTest.java)
