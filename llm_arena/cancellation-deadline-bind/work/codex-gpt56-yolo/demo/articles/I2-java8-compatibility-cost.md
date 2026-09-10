# I2. Java 8 兼容的代价

## 为什么还要支持 Java 8

2026 年了，Java 21 已经发布两年多，虚拟线程、结构化并发、Record 类等特性让人心动。但现实是：大量企业生产环境仍然运行在 Java 8 上。

原因很简单——升级成本太高。一个典型的 Java 8 应用可能依赖上百个第三方库，其中不乏已经停止维护的组件。升级到 Java 11 需要处理 `javax` 到 `jakarta` 的包名迁移、移除的 Java EE 模块、以及被封装的内部 API。更不用说 Java 17 和 21 带来的更强封装和废弃 API 清理。对很多团队来说，这些迁移工作需要数月的测试和验证，而业务需求不允许停下来做这种"无业务价值"的重构。

`parallel-in-scope` 选择 Java 8 作为最低版本，就是为了覆盖这些真实场景。一个在 Java 8 上运行良好的并发工具，自然也能在 Java 11、17、21 上运行。反过来则不行。

## 付出了什么代价

### 没有 CompletableFuture.allOf 的取消能力

Java 8 引入了 `CompletableFuture`，但 `CompletableFuture.allOf()` 的取消语义令人失望——取消一个任务不会影响其他任务，也没有内置的超时机制。Java 9 加强了 `CompletableFuture`，Java 19 引入了结构化并发（`StructuredTaskScope`），原生支持任务组的取消传播和超时控制。

但在 Java 8 中，这些都不存在。我们必须从零构建取消子系统：`CancellationToken`、`Checkpoints`、延迟绑定的超时和失败快速取消机制。

### 没有 var 和 Records

Java 10 的 `var` 让局部变量声明更简洁，Java 16 的 `Record` 让不可变数据类不再需要手写 `equals`、`hashCode`、`toString`。在 Java 8 中，我们只能用 Builder 模式和 Lombok 来弥补。

`BatchExecutionOptions` 的构建就是典型例子：

```java
// Java 16+ 可以用 Record
BatchExecutionOptions opts = new BatchExecutionOptions("task", 5, 3000, TaskType.IO_BOUND);

// Java 8 必须用 Builder
BatchExecutionOptions opts = BatchExecutionOptions.of("task")
        .parallelism(5)
        .timeout(java.time.Duration.ofMillis(3000))
        .taskType(TaskType.IO_BOUND)
        .build();
```

Builder 模式更冗长，但更灵活——可以设置默认值、添加校验逻辑、支持链式调用。这算是"被迫优雅"。

### 没有虚拟线程

Java 21 的虚拟线程（Virtual Threads）让 IO 密集型任务的并发模型彻底改变。一个虚拟线程池可以轻松创建百万级线程，不再需要手动管理线程池大小。

在 Java 8 中，我们必须依赖传统线程池，并用 `ConcurrentLimitExecutor` 的滑动窗口机制来控制并发数。这比虚拟线程更复杂，但至少在 Java 8 环境下是可靠的。

## 用什么弥补

### Guava ListenableFuture

`CompletableFuture` 在 Java 8 中功能有限，我们选择了 Guava 的 `ListenableFuture` 作为基础。它提供了：

- `FluentFuture.withTimeout()` —— 原生超时支持，不需要手动 `get(timeout)`
- `Futures.allAsList()` —— 批量等待，配合取消传播
- `Futures.addCallback()` —— 非阻塞回调，避免线程浪费

这些能力在 Java 8 上就能用，而且 Guava 的维护质量有保障。

### Alibaba TTL

`TransmittableThreadLocal`（TTL）解决了跨线程上下文传播的难题。在 Java 21 中，虚拟线程的 `InheritableThreadLocal` 有更好的支持，但在 Java 8 的传统线程池中，任务提交到线程池后上下文就丢失了。

TTL 通过字节码增强，在任务提交时自动捕获上下文，在任务执行时自动恢复。`CancellationToken`、`BatchExecutionOptions`、任务名称等信息都通过 TTL 在父子线程间隐式传递，开发者无需手动搬运。

### Builder 模式替代 Records

`BatchExecutionOptions`、`GlobalPar` 等配置类全部采用 Builder 模式。虽然比 Record 多了不少代码，但带来了额外的好处：

- 默认值可以内置在 Builder 中，用户只需设置差异化的选项
- 校验逻辑可以在 `build()` 中集中处理，提前发现配置错误
- 链式调用的可读性其实不比 Record 差

## 取舍

支持 Java 8 的代价是真实的：更多的样板代码、更复杂的内部实现、无法利用现代 Java 的语法糖。但换来的是更广的覆盖范围——一个库在 Java 8 上能用，意味着它能服务于绝大多数企业项目。

这不是"落后"，而是务实。当你的用户还在 Java 8 上挣扎时，强迫他们升级才能用你的工具，只会让他们选择不用。更好的策略是：先在 Java 8 上提供可靠的能力，等用户自然升级到更高版本时，再逐步引入现代特性。

`parallel-in-scope` 目前的 API 设计已经为未来做好了准备——当 Java 21 成为主流时，可以无缝替换底层实现，而上层 API 保持不变。

---

> 📁 相关文章：[The Zen of Parallel-in-Scope](https://github.com/huatalk/parallel-in-scope/blob/main/doc/the-zen-of-parallel-in-scope.md)