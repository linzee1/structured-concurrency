# 扩展性与包装契约

> 本文定义 parallel-in-scope 的**扩展接缝**：用户如何在不破坏结构化并发语义的前提下，给任务体加上自己的横切能力（MDC、追踪、指标、重试）。
> 与 [first-principles.md](first-principles.md) 的关系：本文是判据 2（能否参数化现有机制）与判据 4（是否引入第二套执行管道）在"包装"主题上的具体化。
> **状态：设计定稿，`TaskDecorator` 尚未实现。** §5.5 的 L3 检测修的是现存缺陷，可独立先行。

## 0. 一句话结论

**只开放一个用户接缝，位置在任务体（`Callable`）层，且落在库的 `TtlCallable` 之内；库永远是最后一个包装者。**
线程池包装是用户对自有资源的构造行为，不构成库的扩展接口；future 包装被现有不变量直接排除。

## 1. 目标与非目标

### 1.1 目标

| 编号 | 目标 |
|---|---|
| G1 | 用户可插入自己的任务体装饰（MDC/普通 ThreadLocal、追踪、指标、重试） |
| G2 | TTL 回放是任务体栈的最外层，由**构造**保证，不靠文档约定 |
| G3 | 只有一个用户接缝、一个组合点；batch 元素、group 成员、terminal combine 三条路径语义一致 |
| G4 | 不牺牲任何现有结构化语义：取消、deadline、fail-fast、上下文恢复、outcome 归因 |

### 1.2 非目标

- 不提供 executor 包装 SPI（JDK 的 `ExecutorService` 已经是那个扩展点）；
- 不提供 future 包装接缝（需要 future 级能力就扩展库内类型）；
- 不承诺"整条执行路径上 TTL 恰好捕获一次"（见 §2 的 I2）；
- 不新增 `TaskOutcome` 词汇。

## 2. 参照系：两个边界与两个不变量

worker 线程上的真实调用栈（现状，已实现）：

```
executor.execute(future)                    ← 提交线程；SubmissionScope 已 install
  └─ [worker] future.run()                  ← ExecutionPhaseHintFuture：phase CAS → notifyPhase(RUNNING)
      └─ TtlCallable.call()                 ← 库最后一次包装：replay(captured) … restore(backup)
          └─ ScopedCallable.call()          ← install TaskExecutionContext → checkpoint → delegate
              └─ 用户装饰器 N … 装饰器 1      ← 本文要定义的用户接缝
                  └─ 用户 Callable / lambda
```

两个边界必须分清：

- **executor 调度边界**：`executor.execute()` 之前/之后、`run()` 之外的一切。归用户所有，库不控制。
- **任务体边界**：`ScopedCallable.call()` 起的一切。归库控制。

由此得到两个不变量，强弱完全不同：

| 编号 | 内容 | 可保证性 |
|---|---|---|
| **I1** | 库的 TTL 回放（`replay`/`restore`）包住任务体边界内的整条链 | ✅ 由构造保证 |
| **I2** | 整条执行路径上 TTL 捕获恰好一次 | ❌ 只能检测/告警 |

**I2 无法保证的原因**：池归用户所有，用户可以在 `run()` 之外再包一层（`TtlExecutors.getTtlExecutorService` 给每个 Runnable 套 `TtlRunnable`），或启用 TTL agent。此时库的 I1 仍成立，但路径上出现第二个捕获点。
**所有关于"最外层"的设计讨论，指的都是 I1；把 I2 当成可保证的不变量是设计错误。**

## 3. 三个轴：用户可能包装的三个对象

| 候选 | 所属轴 | 谁拥有 | 影响 I1 | 影响 I2 | 库内现状 |
|---|---|---|---|---|---|
| 线程池 | 提交机制轴 | 用户（`register` 之前） | ❌ 不在任务体栈里 | ✅ 可加外部捕获点 | `ExecutorRuntime` |
| Callable | 任务体轴 | 库（构造）/ 用户（内容） | ✅ 决定性 | ✅ 可控制 | `TaskSubmissions.wrapScoped` |
| FutureTask | 结果与生命周期轴 | 库 | — | — | `ExecutionPhaseHintFuture` |

**第四个轴**：`ThreadFactory`（线程创建轴）。线程命名、优先级、inheritable 语义属于此轴，不属于任务包装。

**判据**：per-task 且需要任务身份 → 任务体轴；per-pool → 池构造期；不是你拥有的代码 → JVM agent；future 语义 → 必须进库。

## 4. 必须避免的问题

按严重度排列。每条给出触发机制与封堵手段。

| 编号 | 问题 | 触发机制 | 后果 | 封堵手段 |
|---|---|---|---|---|
| **P1** | 顺序靠文档约定 | 要求用户"最后包 TTL" | 用户会忘（公理 2），顺序静默错乱 | 库是唯一最后包装者（L1） |
| **P2** | 轴错位 | 用 executor 包装实现任务体功能 | 装饰器拿不到任务身份、不计时、不归因 | §3 判据 + 不提供 executor SPI |
| **P3** | 注册 TTL 包装器 | `TtlExecutors.getTtlExecutorService(pool)` 传入 `register` | ① 出现第二个捕获点（破 I2）；② `ExecutorServiceTtlWrapper` 非 `ThreadPoolExecutor` → purge 观察者不绑定、`BlockingRisk` 静默降为 `UNKNOWN` | L3 检测 + WARNING；能力探测时 `TtlUnwrap.unwrap` |
| **P4** | 包装 future | 自建 `Callable`/`FutureTask` 替换 `ExecutionPhaseHintFuture` | phase 状态机丢失、purge 观察者丢失、`cancel(true)` 中断失效、批次的 future 恒等被破坏 | 不开放 future 接缝；公开 API 不收已准备对象（L2） |
| **P5** | 装饰器落在 `ScopedCallable` 之外 | 在库的构造点之后插入用户包装 | 读不到 `TaskExecutionContext.current()`；耗时不入 timing；在 `checkpoint` 前抛异常时 listener 完全收不到事件 | 接缝固定在最内层（L1/L2） |
| **P6** | 装饰器吞异常/吞中断 | 装饰器 `catch` 后返回正常值 | `TaskOutcome` 归因失真；取消变成"成功" | 契约 C3 |
| **P7** | 在 `call()` 时捕获普通 `ThreadLocal` | 误以为 TTL 会传播一切 | 捕获到 worker 线程的残留值，而不是提交线程的值 | 契约 C2（`decorate()` 是唯一捕获时点） |
| **P8** | 重试重跑外层包装 | 装饰器重新调用外层 `TtlCallable` | `IllegalStateException: TTL value reference is released after call!`（`releaseTtlValueReferenceAfterCall=true`） | 契约 C5：只重跑传入的 `task`，且重试前自查 token |
| **P9** | 两个包装点 | batch 与 group 各写一套装饰逻辑 | 语义分叉，第三方能力只覆盖一半路径 | 唯一构造点 `TaskSubmissions.wrapScoped`（L1） |
| **P10** | 忽略 inline 路径 | 装饰器假设"捕获线程 ≠ 执行线程" | CPU-bound 拒绝回退时行为不一致 | 契约 C6 + 验证矩阵 |
| **P11** | 提供关闭/替换 TTL 包装的开关 | 为"我自己处理 TTL"加配置 | 直接破 I1，且制造第二条执行路径 | 明确不提供（L4） |
| **P12** | 把 `TaskDecorator` 做成泛型 functional interface | `interface TaskDecorator<T> { Callable<T> decorate(Callable<T>); }` + `List<TaskDecorator<?>>` | 需要一次 unchecked cast；用户可注册类型不匹配的装饰器 → 堆污染 | 抽象类 + 泛型方法（§5.1） |

## 5. 最优实现

### 5.1 接口形态：抽象类，不是 `@FunctionalInterface`

```java
package io.github.monadrome.parallelinscope.spi;

/**
 * 任务体装饰器：库在 {@code ScopedCallable} 之内、{@code TtlCallable} 之外应用。
 * 注册序 = 由外到内。实例可能被多任务并发使用，实现必须线程安全。
 */
public abstract class TaskDecorator {
    public abstract <T> Callable<T> decorate(Callable<T> task);
}
```

**为什么不能用 lambda**：javac 1.8 实测——lambda 不能实现泛型抽象方法：

```
错误: 不兼容的类型: lambda 表达式的函数描述符无效
    接口 GenericFn 中的方法 <T>(Callable<T>)Callable<T> 为泛型方法
```

用户写匿名子类，与 `TaskKey` 同构（`new TaskKey<List<Order>>("orders") {}`），项目内已有先例。

**为什么不是 `TaskDecorator<T>`**：注册面是异构的（同一 `Par` 下的 batch 元素、group 成员、combine 结果类型各不相同），只能存 `List<TaskDecorator<?>>`，应用时需一次 unchecked cast。用户可注册 `TaskDecorator<String>` 到一个 `Callable<Integer>` 的成员上 → 堆污染、运行期 `ClassCastException`。泛型方法没有这个洞（公理 3：安全优先于表达力）。

### 5.2 唯一构造点

```java
// internal/TaskSubmissions.wrapScoped —— 全库唯一调用 TtlCallable.get 的地方
public static <V> Callable<V> wrapScoped(
        TaskExecutionContext taskContext,
        Callable<V> userCallable,
        List<TaskListener> taskListeners,
        List<TaskDecorator> decorators) {
    Callable<V> body = userCallable;
    for (int i = decorators.size() - 1; i >= 0; i--) {   // 倒序 → 先注册的在最外
        body = decorators.get(i).decorate(body);
    }
    return TtlCallable.get(new ScopedCallable<>(taskContext, body, taskListeners), true, true);
}
```

**循环方向是易错点**：`body = d.decorate(body)` 会让最后遍历到的成为最外层，因此必须倒序遍历，才能实现"注册序 = 由外到内"。

三条路径自动一致（现状已共用此点）：

| 路径 | 入口 |
|---|---|
| batch 元素 | `Par.executeGlobal` → `TaskSubmissions.prepare` |
| group 成员 | `TaskGroup` → `Par.prepareGroupTask` → `TaskSubmissions.prepare` |
| terminal combine | 同上（combine 也是 scoped task） |

batch 的 `Function` 在 `Par.mapWhileOpen` 已转成每元素 `Callable`，因此 `Callable` 级接口天然覆盖三条路径。

### 5.3 注册面

镜像已有的 listener 设计（`GlobalPar.Builder.taskListener` / `parTaskListener` / `GlobalPar.taskListenersFor`）：

```java
GlobalPar.Builder
    .taskDecorator(TaskDecorator)              // 全局默认，按注册序追加
    .parTaskDecorator(ParName, TaskDecorator)  // 按 Par 追加，位于全局之后
GlobalPar.taskDecoratorsFor(ParName)           // 与 taskListenersFor 对称
```

**组合语义是追加，不是 listener 的覆盖替换**：静默丢弃一个传播型装饰器属于"忘记"类错误。要少用就不全局注册；这个差异 MUST 写进用户文档。

### 5.4 四层保证

| 层 | 手段 | 作用 |
|---|---|---|
| L0 | 文档约定"用户必须最后包 TTL" | **否决**——用户会忘 |
| **L1 结构** | 库是唯一最后包装者；用户接缝在最内层 | 主保证 |
| **L2 类型** | 公开 API 只收 `Callable`/`Function`，不收"已准备对象"；`internal` 非 API，用架构测试禁止外部引用 | 防绕过 |
| **L3 运行时** | 检测 TTL 包装器 + WARNING；不提供关闭/替换 TTL 包装的开关 | 防配错 |
| **L4 测试** | §8 验证矩阵 | 防回归 |

### 5.5 L3 检测（可独立先行，修现存缺陷）

```java
// ExecutorRuntime 构造期
if (TtlUnwrap.isWrapper(suppliedExecutor)) {
    LOGGER.warning("Registered executor is a TTL wrapper; register the physical pool instead. "
            + "TTL capture will happen twice (executor boundary + prepare), and purge/BlockingRisk "
            + "introspection is disabled.");
}
// 能力探测（purge、BlockingRisk）在解包后的对象上进行
ExecutorService introspectable = TtlUnwrap.unwrap(suppliedExecutor);
```

事实依据（TTL 2.14.5 源码逐条验证）：

- `TtlExecutors.getTtlExecutorService` 在 `TtlAgent.isTtlAgentLoaded() || executor instanceof TtlEnhanced` 时**直接返回原对象**——同一份代码在不同部署下行为不同；
- `ExecutorServiceTtlWrapper` 是包私有类（`implements ExecutorService, TtlEnhanced`），外部无法 `instanceof`，但 `TtlUnwrap.isWrapper/unwrap` 是公开 API；
- Guava 的 `WrappingExecutorService` 是包私有且无公开解包入口，因此**只修 TTL 一侧**；
- **身份不跟着解包**：`ExecutorIdentity` 仍按注册对象（见其 javadoc 的理由），否则同一物理池以两个对象注册会被合并，改变执行器图语义。

## 6. 契约

### 6.1 `TaskDecorator` 实现方契约

| 编号 | 契约 |
|---|---|
| C1 | **线程安全/无状态**：一个实例可能服务多个任务、多个线程；不得持有每次调用的可变状态 |
| C2 | **捕获时点**：`decorate()` 在 prepare 阶段、提交线程上调用恰好一次。需要传播的普通 `ThreadLocal`/MDC MUST 在此刻捕获；在 `call()` 时读取会拿到 worker 线程的值 |
| C3 | **异常**：MUST NOT 吞异常；MUST NOT 把 `CancellationException`/`InterruptedException` 转成正常返回。装饰器抛出的异常归因为 `USER_FAILURE` |
| C4 | **阻塞**：MUST NOT 阻塞提交线程；MUST NOT 调用 `Future.get()` |
| C5 | **重试**：重试 MUST 只重跑传入的 `task`（外层 `TtlCallable` 只能 `call()` 一次）；MUST 在每次重试前检查取消 token，否则破坏协作式取消 |
| C6 | **不得假设跨线程**：inline 路径（CPU-bound 拒绝回退）上捕获与执行可能在同一线程 |

重试语义的推论（MUST 写进文档）：一次任务 = 一次 checkpoint、一次 timing 窗口、一次 listener 事件、N 次用户执行。

### 6.2 库侧契约

| 编号 | 契约 |
|---|---|
| L1 | **唯一构造点**：全库只有 `TaskSubmissions.wrapScoped` 调用 `TtlCallable.get` |
| L2 | **顺序**：`TtlCallable( ScopedCallable( decorators( user ) ) )`；注册序 = 由外到内 |
| L3 | **单次应用**：每个任务恰好被每个装饰器包装一次；batch/member/combine 共用同一构造点 |
| L4 | **不可配置**：不提供关闭或替换 TTL 包装的配置项 |
| L5 | **归因不变**：装饰器不引入新的 `TaskOutcome` 词汇 |

### 6.3 用户侧约束

- MUST NOT 通过 executor 包装把 Runnable 再包一层（尤其 `TtlExecutors`）；
- MUST NOT 依赖 `internal` 包类型自建执行管道；
- MUST NOT 期望 TTL 传播普通 `ThreadLocal` 或 MDC——TTL 只传播 `TransmittableThreadLocal`。

## 7. 不变量

| 编号 | 不变量 | 校验方式 |
|---|---|---|
| INV-1 | 库的 TTL 回放包住任务体边界内的整条链 | 装饰器内读取 `TransmittableThreadLocal` == 提交线程的值 |
| INV-2 | 生命周期层在用户装饰器之外 | 装饰器内 `TaskExecutionContext.current()` 非空 |
| INV-3 | 每任务恰好被每个装饰器包装一次 | 装饰器计数 == 任务数 |
| INV-4 | 用户代码不出现在 `TtlCallable` 之外 | 顺序断言 + 架构测试（禁止 `internal` 外部引用） |
| INV-5 | 装饰器异常归因 `USER_FAILURE`，不新增词汇 | 异常装饰器 → 批/组 outcome 断言 |
| INV-6 | 取消/deadline/fail-fast 语义与是否注册装饰器无关 | 带装饰器的取消/fail-fast/timeout 测试 |
| INV-7 | inline 路径与正常路径顺序相同 | 拒绝回退测试 |
| INV-8 | 顺序确定：注册序 = 由外到内 | 多装饰器 enter/exit 序列断言 |
| INV-9 | 能力探测不改变身份 | `ExecutorIdentity` 仍 == 注册对象 |

## 8. 验证矩阵

| 编号 | 用例 | 断言 |
|---|---|---|
| T1 | 顺序 | 记录 enter/exit → `[ttl, scoped, d_N, …, d_1, body]` |
| T2 | 上下文可见性 | 装饰器内 TTL 可见、普通 `ThreadLocal` 不可见、`TaskExecutionContext.current()` 非空 |
| T3 | 三路径一致 | batch / group member / combine 各装饰一次，顺序相同 |
| T4 | 注册面 | 全局 + per-Par 同时注册 → 各应用一次，全局在外 |
| T5 | 异常 | 装饰器抛异常 → `USER_FAILURE`；listener 收到失败事件 |
| T6 | 取消 | 装饰器内 token 已取消时任务不进入用户代码；重试装饰器自查 token |
| T7 | inline | 拒绝回退路径顺序与正常路径相同 |
| T8 | 不变量 | 装饰器内 `call()` 外层 TtlCallable 的重复调用抛出 `IllegalStateException`（负例断言） |

## 9. 明确不做

| 方案 | 否决理由 |
|---|---|
| executor 包装 SPI | JDK 的 `ExecutorService` 已是扩展点；再加一层只会让身份与内省问题更隐蔽（P3） |
| future 包装接缝 | 破坏 phase/purge/取消/恒等（P4）；future 级能力应进库扩展 |
| 关闭/替换 TTL 包装的开关 | 直接破 I1（P11） |
| `WrapperChain`/`Wrapper` 组合类型 | `List<TaskDecorator>` 顺序应用已足够；新概念不消除任何一类错误（判据 2） |
| 可选的 Listening 包装 | `ListenableFuture` 是内部实现细节；对外契约是 `TaskBatchResult`/`TaskGroupResult` |
| `MultiTaskOptions.taskDecorator(...)` | 把行为塞进纯执行参数，且组内每个成员重复配置；按 Par 注册一次即可 |
| `TaskDecorator<T>` + 通配列表 | 堆污染（P12） |

## 10. 落地顺序

1. **L3 检测（可独立先行）**：`ExecutorRuntime` 加 `TtlUnwrap.isWrapper` 告警 + 能力探测解包 + 回归测试；
2. 本文档 + `design/AGENTS.md` 索引 + `CHANGELOG.md` 记录；
3. `TaskDecorator` SPI + `TaskSubmissions.wrapScoped` 参数化 + `GlobalPar` 注册面；
4. §8 验证矩阵；
5. 用户文档（中英）：顺序、捕获时点、重试语义、注册面追加语义。

## 11. 参考

- [first-principles.md](first-principles.md)：公理与评估判据
- [../docs/zh/design/idea-graveyard.md](../docs/zh/design/idea-graveyard.md)：否决记录
- [task-group-submission.md](task-group-submission.md) §7.3、§9：单任务提交内核的复用边界
- [task-group-lifecycle.md](task-group-lifecycle.md) §4.8：TTL 边界
- `internal/TaskSubmissions`、`internal/ScopedCallable`、`internal/ExecutionPhaseHintFuture`、`scope/ExecutorRuntime`、`scope/ExecutorIdentity`
- TTL 2.14.5：`TtlCallable`（`get` 的 idempotent 语义、`releaseTtlValueReferenceAfterCall`）、`TtlExecutors`（agent 短路）、`TtlUnwrap`
