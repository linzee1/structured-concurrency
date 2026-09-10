**中文** | [**English**](../en/README.md)

# Demo 文章索引

这些文章是基于 `0.2.0-SNAPSHOT` API 的问题导向示例；公共契约以[完整使用指南](../../../docs/zh/user-guide.md)和[协作式取消说明](../../../docs/zh/reference/cooperative-cancellation.md)为准。

## 推荐起点

- [5 分钟快速上手](articles/QUICK-START-5-minutes.md)
- [批量调用最佳实践](articles/BATCH-best-practices.md)

## 取消与超时

- [A1. cancel(true) 被忽略](articles/A1-cancel-true-ignored.md)
- [A2. 嵌套任务取消传播](articles/A2-nested-cancel-propagation.md)
- [A3. Lean 与 Fat 取消异常](articles/A3-lean-vs-fat-exception.md)
- [D1. get(timeout) 后任务仍运行](articles/D1-get-timeout-task-still-running.md)
- [D2. 延迟绑定竞态](articles/D2-late-bind-race-condition.md)
- [G3. 协作式取消检查点](articles/G3-checkpoints-cooperative-cancel.md)

## 上下文与 API

- [B1. ThreadLocal 上下文丢失](articles/B1-threadlocal-context-lost.md)
- [B2. 函数签名膨胀](articles/B2-function-signature-bloat.md)
- [H1. null 与空列表处理](articles/H1-defensive-null-handling.md)

## 线程池与调度

- [C1. 固定线程池嵌套死锁](articles/C1-thread-pool-deadlock.md)
- [C2. Cached 与 Fixed 线程池](articles/C2-cached-vs-fixed-pool.md)
- [E1. 一次提交导致队列洪峰](articles/E1-queue-flooding.md)
- [E2. 提交循环与业务线程隔离](articles/E2-submitter-pool-offloading.md)
- [F1. CPU 密集任务排队](articles/F1-cpu-task-queuing.md)
- [I3. 滑动窗口与信号量](articles/I3-sliding-window-vs-semaphore.md)

## 监控与集成

- [G1. TaskListener 监控](articles/G1-task-listener-monitoring.md)
- [G2. 批量结果报告](articles/G2-batch-result-report.md)
- [G4. 命名线程池](articles/G4-named-executor-pool.md)
- [G5. 批量 HTTP 调用](articles/G5-batch-http-calls.md)
- [G6. 数据库分片查询](articles/G6-batch-db-query.md)

## 设计边界

- [I1. Idea Graveyard](articles/I1-idea-graveyard.md)
- [I2. Java 8 兼容成本](articles/I2-java8-compatibility-cost.md)
- [I4. 与 CompletableFuture.allOf 对比](articles/I4-vs-completable-future.md)
