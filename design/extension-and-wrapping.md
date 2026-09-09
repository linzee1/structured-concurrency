# 扩展性与包装契约

> 本文定义 parallel-in-scope 的**扩展接缝**：用户如何在不破坏结构化并发语义的前提下，给任务体加上自己的横切能力（MDC、追踪、指标、重试）。
> 与 [first-principles.md](first-principles.md) 的关系：本文是判据 2（能否参数化现有机制）与判据 4（是否引入第二套执行管道）在"包装"主题上的具体化。
> **状态：设计定稿，`TaskDecorator` 尚未实现。** §5.5 的 L3 检测修的是现存缺陷，可独立先行。

## 0. 一句话结论

**唯一合适的用户接缝是任务体（`Callable`）层**：库先折叠用户装饰器，再由库在最外层构造上下文层，然后只创建一个 future，原样提交并返回。

可保证的语义有明确边界：

- **结构保证**：库的上下文回放位于**所有**用户装饰器之外；
- **语义保证**：对装饰器**同步、同线程动态调用范围内**的任务代码生效。

装饰器把 `next` 逃逸到别的线程或别的时刻、依赖普通 `ThreadLocal`、或让 executor 在库之外再包一层，都不在保证内（见 §2 的 I2/I3）。

## 1. 目标与非目标

### 1.1 目标

| 编号 | 目标 |
|---|---|
| G1 | 用户可插入自己的任务体装饰（MDC/普通 ThreadLocal、追踪、指标、重试） |
| G2 | 上下文回放是任务体栈的最外层，由**构造**保证，不靠文档约定 |
| G3 | 只有一个用户接缝、一个组合点；batch 元素、group 成员、terminal combine 三条路径语义一致 |
| G4 | 不牺牲任何现有结构化语义：取消、deadline、fail-fast、上下文恢复、outcome 归因 |

### 1.2 非目标

- 不提供 executor 包装 SPI（JDK 的 `ExecutorService` 已经是那个扩展点）；
- 不提供 future 包装接缝（需要 future 级能力就扩展库内类型）；
- **不承诺覆盖装饰器把任务体逃逸到其它线程/其它时刻的代码**；
- **不承诺传播任意 `ThreadLocal`**（传播集合封闭，见 §2）；
- 不新增 `TaskOutcome` 词汇。

## 2. 参照系：三个前提、两个边界、三个不变量

### 2.1 三个前提（不成立则一切保证失效）

| 编号 | 前提 |
|---|---|
| A1 | **池归用户所有**：库只调用 `Executor.execute()`，不创建、不关闭、不假设实现类 |
| A2 | **用户 executor 遵守 `Executor` 契约**：恰好一次调用传入对象的 `run()`，不丢弃、不重复、不换线程 |
| A3 | **传播集合封闭**：上下文 = 库登记的可传播载体（`TransmittableThreadLocal` 注册集合）；Java 8 无法枚举任意 `ThreadLocal` |

### 2.2 两个边界

worker 线程上的真实调用栈（现状，已实现）：

```
executor.execute(future)                    ← 提交线程；SubmissionScope 已 install
  └─ [worker] future.run()                  ← ExecutionPhaseHintFuture：phase CAS → notifyPhase(RUNNING)
      └─ TtlCallable.call()                 ← 库最后一次包装：replay(captured) … restore(backup)
          └─ ScopedCallable.call()          ← install TaskExecutionContext → checkpoint → delegate
              └─ 用户装饰器 N … 装饰器 1      ← 本文要定义的用户接缝
                  └─ 用户 Callable / lambda
```

- **executor 调度边界**：`executor.execute()` 之前/之后、`run()` 之外。归用户所有，库不控制。
- **任务体边界**：`ScopedCallable.call()` 起的一切。归库控制。

### 2.3 三个不变量，强弱完全不同

| 编号 | 内容 | 可保证性 |
|---|---|---|
| **I1** | 库的上下文回放位于所有用户装饰器之外（结构） | ✅ 由构造保证 |
| **I2** | 对装饰器同步、同线程动态调用范围内的任务代码，回放生效（语义） | ✅ 由契约 + 负例测试保证 |
| **I3** | 整条执行路径上上下文捕获恰好一次 | ❌ 只能检测/告警 |

**I3 无法保证的原因**：池归用户所有（A1），用户可以在 `run()` 之外再包一层（`TtlExecutors.getTtlExecutorService` 给每个 Runnable 套 `TtlRunnable`），或启用 TTL agent。此时 I1/I2 仍成立，但路径上出现第二个捕获点。

**I2 不是 I1 的推论**：装饰器若把 `next.call()` 交给别的线程（`CompletableFuture.supplyAsync`、新线程、另一个池），或保存 `next` 留到外层 `call()` 返回后再调用，上下文回放覆盖不到那段代码。**所有"最外层"的设计讨论指的都是 I1；把 I2/I3 当成可保证的不变量是设计错误。**

## 3. 三个轴：用户可能包装的三个对象

| 候选 | 所属轴 | 谁拥有 | 影响 I1/I2 | 影响 I3 | 库内现状 |
|---|---|---|---|---|---|
| 线程池 | 提交机制轴 | 用户（`register` 之前） | ❌ 不在任务体栈里 | ✅ 可加外部捕获点 | `ExecutorRuntime` |
| Callable | 任务体轴 | 库（构造）/ 用户（内容） | ✅ 决定性 | ✅ 可控制 | `TaskSubmissions.wrapScoped` |
| FutureTask | 结果与生命周期轴 | 库 | — | — | `ExecutionPhaseHintFuture` |

**第四个轴**：`ThreadFactory`（线程创建轴）。线程命名、优先级、inheritable 语义属于此轴，不属于任务包装。

### 3.1 选择接缝的判据

1. 捕获点必须**逐任务且由库控制**；
2. 强制上下文层必须能固定在**所有**用户包装之外；
3. 不得改变或复制 future 的状态机；
4. 不得扩张线程池所有权；
5. 必须支持结果泛型、异常传播与同步重试；
6. 用户不得取得最终组装对象并重排强制层。

**只有 `Callable` 同时满足六条。**

### 3.2 各轴结论

- **线程池**：可以包，但它是"谁来跑"的机制轴，不参与任务体排序。三个副作用：① `TtlExecutors` 会在 `run()` 之外再加一个捕获点（破 I3）；② 会把 future 的内部记账与完成回调也纳入上下文范围；③ 包装器若吞掉 `RejectedExecutionException` 或丢弃任务，future 可能永不完成（破 A2）。它的**唯一价值域是"库之外的提交"**——库内提交经它会被双重捕获。
- **FutureTask**：不是排序问题，是被不变量排除。它是一次性状态机与结果对象，代理它会产生两套取消/完成状态，重试也无法复用。需要 future 级能力 → 扩展库内类型。
- **Callable**：唯一正确接缝。精确对应任务体，不碰池所有权，不改变 future 身份，天然支持多层包装与重试。

**注册粒度是 scope 级（按 `Par`），不是逐任务**：结果类型在 scope 内异构（batch 元素、成员、combine 各不相同），逐任务配置既无法用一个接口表达，也会让组内每个成员重复配置。

## 4. 必须避免的问题

### A. 顺序与位置

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P1** | 顺序靠文档约定 | 要求用户"最后包上下文" | 用户会忘（公理 2），顺序静默错乱 | 库是唯一最后包装者（L1） |
| **P2** | 轴错位 | 用 executor 包装实现任务体功能 | 装饰器拿不到任务身份、不计时、不归因 | §3 判据 + 不提供 executor SPI |
| **P3** | 装饰器位置错误 | 装饰器被放在上下文层之外或生命周期层之外 | 读不到 `TaskExecutionContext.current()`；耗时不入 timing；在 `checkpoint` 前抛异常时 listener 收不到事件 | 接缝固定在最内层（L1/L2） |
| **P4** | 两个包装点 | batch 与 group 各写一套装饰逻辑 | 语义分叉，第三方能力只覆盖一半路径 | 唯一构造点 `TaskSubmissions.wrapScoped`（L1） |

### B. 上下文

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P5** | 在 `call()` 时捕获普通 `ThreadLocal` | 误以为上下文会自动传播 | 捕获到 worker 线程的残留值，而非提交线程的值 | 契约 C2（`decorate()` 是唯一捕获时点） |
| **P6** | 装饰器把 `next` 逃逸 | `CompletableFuture.supplyAsync`、新线程、另一个池，或保存 `next` 到外层返回后再调用 | 回放只覆盖原 worker 的同步动态范围，逃逸段读到脏上下文（破 I2） | 契约 C6 + 负例测试 T7 |
| **P7** | 恢复时清空而非还原 | 后置逻辑写 `null` 而不是捕获到的原值 | 污染池线程，影响后续无关任务 | 契约 C7 |
| **P8** | 快照跨任务复用 | 多次提交共享同一捕获对象 | 陈旧值、并发污染、可变对象泄漏 | 库侧 L6：每次 prepare 独立捕获 |

### C. 结果与生命周期

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P9** | 包装 future | 自建 `Callable`/`FutureTask` 替换 `ExecutionPhaseHintFuture` | phase 状态机丢失、purge 观察者丢失、`cancel(true)` 中断失效、批次的 future 恒等被破坏 | 不开放 future 接缝；公开 API 不收已准备对象（L2） |
| **P10** | 调用 `executor.submit()` 而非 `execute()` | 想复用 JDK 的提交便利 | executor 再包一层 FutureTask → 返回的 future 与实际执行的 future 分裂，取消/结果/监听器不一致 | 库侧 L7：只调用 `execute()` |

### D. executor 轴

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P11** | 注册 TTL 包装器 | `TtlExecutors.getTtlExecutorService(pool)` 传入 `register` | ① 出现第二个捕获点（破 I3）；② `ExecutorServiceTtlWrapper` 非 `ThreadPoolExecutor` → purge 观察者不绑定、`BlockingRisk` 静默降为 `UNKNOWN` | L3 检测 + WARNING；能力探测时 `TtlUnwrap.unwrap` |
| **P12** | executor 包装吞掉拒绝或违反契约 | 包装器内部消化 `RejectedExecutionException`、丢弃任务、重复执行、换线程执行 | future 永不完成或重复执行（破 A2）；库的 CPU-bound inline 回退策略被绕过 | 契约 U2 + 文档；负例测试 T11 |
| **P13** | executor 包装扩大上下文范围 | 包装器给每个 Runnable 套上下文层 | 上下文回放覆盖到 future 记账与完成回调，语义超出"任务体" | 契约 U1：不在 `run()` 之外包上下文 |

### E. 接口与类型

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P14** | `TaskDecorator<T>` + 通配列表 | 想让接口能用 lambda | 需要一次 unchecked cast；用户可注册类型不匹配的装饰器 → 堆污染 | 泛型方法（§5.1） |
| **P15** | 装饰器返回 `null` 或类型不兼容 | 包装逻辑写错 | 失败位置远离配置错误，延迟到 worker 才暴露 | 契约 C8（构造期校验 + 立即失败） |

### F. 异常、取消与配置

| 编号 | 问题 | 触发机制 | 后果 | 封堵 |
|---|---|---|---|---|
| **P16** | 吞异常或吞中断且不恢复中断标志 | 装饰器 `catch` 后返回正常值 | `TaskOutcome` 归因失真；`cancel(true)` 失效，scope 无法收敛 | 契约 C3 |
| **P17** | 重试重跑外层包装 | 装饰器重新调用外层上下文包装 | `IllegalStateException: TTL value reference is released after call!` | 契约 C5：只重跑传入的 `next` |
| **P18** | 忽略 inline 路径 | 装饰器假设"捕获线程 ≠ 执行线程" | CPU-bound 拒绝回退时行为不一致 | 契约 C9 + 验证矩阵 T10 |
| **P19** | 提供关闭/替换上下文包装的开关 | 为"我自己处理上下文"加配置 | 直接破 I1，且制造第二条执行管道 | 明确不提供（L4） |

## 5. 最优实现

### 5.1 接口形态：泛型方法接口，不是 lambda 目标

```java
package io.github.monadrome.parallelinscope;

/**
 * 任务体装饰器：库在 {@code ScopedCallable} 之内、上下文层之外应用。
 * 注册序 = 由外到内。实例可能被多任务并发使用，实现必须线程安全。
 *
 * <p>方法级泛型使同一实例可服务异构结果类型；代价是不能用 lambda 实现
 * （javac：lambda 表达式的函数描述符无效——泛型方法不能作为 lambda 的目标），
 * 实现方使用匿名类或具名类。
 */
public interface TaskDecorator {
    <V> Callable<V> decorate(Callable<V> next);
}
```

**为什么不加 `@FunctionalInterface`**：注解本身合法，但会误导用户尝试 lambda 而编译失败（javac 1.8 实测报"lambda 表达式的函数描述符无效"）。

**为什么不是 `TaskDecorator<T>`**：注册面异构（同一 `Par` 下 batch 元素、group 成员、combine 结果类型各不相同），只能存 `List<TaskDecorator<?>>`，应用时需一次 unchecked cast。用户可注册 `TaskDecorator<String>` 到 `Callable<Integer>` 的成员上 → 堆污染、运行期 `ClassCastException`。泛型方法没有这个洞（公理 3：安全优先于表达力）。

**为什么是接口不是抽象类**：既有回调（`TaskListener`/`TaskGroupListener`/`DeadlockDetectionListener`）
都是接口；`TaskKey` 用抽象类是因为需要匿名子类捕获类型参数，这里没有该需求。

### 5.2 唯一构造点

```java
// package-private TaskSubmissions.wrapScoped —— 全库唯一调用 TtlCallable.get 的地方
public static <V> Callable<V> wrapScoped(
        TaskExecutionContext taskContext,
        Callable<V> userCallable,
        List<TaskListener> taskListeners,
        List<TaskDecorator> decorators) {
    Callable<V> body = userCallable;
    for (int i = decorators.size() - 1; i >= 0; i--) {   // 倒序 → 先注册的在最外
        Callable<V> wrapped = decorators.get(i).decorate(body);
        if (wrapped == null) throw new IllegalArgumentException("TaskDecorator returned null");
        body = wrapped;
    }
    return TtlCallable.get(new ScopedCallable<>(taskContext, body, taskListeners), true, true);
}
```

**循环方向是易错点**：`body = d.decorate(body)` 会让最后遍历到的成为最外层，因此必须倒序遍历，才能实现"注册序 = 由外到内"。

**装饰器列表是构建期冻结的有序不可变快照**（`GlobalPar` 不可变），提交时不再变化——避免并发迭代与顺序抖动。

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
| L0 | 文档约定"用户必须最后包上下文" | **否决**——用户会忘 |
| **L1 结构** | 库是唯一最后包装者；用户接缝在最内层 | 主保证（I1） |
| **L2 类型** | 公开 API 只收 `Callable`/`Function`，不收"已准备对象"；已准备对象的类型和构造入口必须 package-private，用公开 API 白名单测试禁止内核类泄漏 | 防绕过（P9） |
| **L3 运行时** | 检测 TTL 包装器 + WARNING；不提供关闭/替换上下文包装的开关 | 防配错（P11/P19） |
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
| C2 | **捕获时点**：`decorate()` 在 prepare 阶段、提交线程上调用恰好一次。需要传播的普通 `ThreadLocal`/MDC MUST 在此刻捕获；在 `call()` 时读取会拿到 worker 线程的值（P5） |
| C3 | **异常**：MUST NOT 吞异常；MUST NOT 把 `CancellationException`/`InterruptedException` 转成正常返回；捕获中断后 MUST 恢复中断标志。装饰器抛出的异常归因为 `USER_FAILURE` |
| C4 | **阻塞**：MUST NOT 阻塞提交线程；MUST NOT 调用 `Future.get()` |
| C5 | **重试**：重试 MUST 只重跑传入的 `next`（外层上下文包装只能 `call()` 一次）；MUST 在每次重试前检查取消 token，否则破坏协作式取消 |
| C6 | **不得逃逸**：MUST 在本次 `call()` 的同步、同线程动态范围内调用 `next`；MUST NOT 交给其它线程/其它池，MUST NOT 保存 `next` 到外层返回后再调用（P6） |
| C7 | **精确恢复**：装饰器自己设置的线程本地状态 MUST 恢复为捕获到的原值，而非清空（P7） |
| C8 | **返回非 null 且类型兼容**：`decorate` 返回 null 或结果类型不兼容时，库在构造期立即失败（P15） |
| C9 | **不得假设跨线程**：inline 路径（CPU-bound 拒绝回退）上捕获与执行可能在同一线程（P18） |

重试语义的推论（MUST 写进文档）：一次任务 = 一次 checkpoint、一次 timing 窗口、一次 listener 事件、N 次用户执行。

**可见性推论**：装饰器的 `finally` 不保证在 future 完成通知之前执行——取消竞态下 future 可能先进入终态。装饰器 MUST NOT 依赖"future 已终态 ⇒ 我的后置逻辑已跑完"。

### 6.2 库侧契约

| 编号 | 契约 |
|---|---|
| L1 | **唯一构造点**：全库只有 `TaskSubmissions.wrapScoped` 调用 `TtlCallable.get` |
| L2 | **顺序**：`Context( ScopedCallable( decorators( user ) ) )`；注册序 = 由外到内 |
| L3 | **单次应用**：每个任务恰好被每个装饰器包装一次；batch/member/combine 共用同一构造点 |
| L4 | **不可配置**：不提供关闭或替换上下文包装的配置项 |
| L5 | **归因不变**：装饰器不引入新的 `TaskOutcome` 词汇 |
| L6 | **快照独立**：每次 `prepare` 独立捕获上下文快照，不跨任务复用（P8） |
| L7 | **只用 `execute()`**：向用户 executor 提交 MUST 调用 `execute(Runnable)`，MUST NOT 调用 `submit()`（P10）；被拒绝时 MUST 终结 future（inline 回退或 `SubmissionException`），不得返回永不完成的对象 |

### 6.3 用户侧约束

| 编号 | 约束 |
|---|---|
| U1 | MUST NOT 通过 executor 包装把 `run()` 之外的 Runnable 再包一层（尤其 `TtlExecutors`） |
| U2 | 注册的 executor MUST 遵守 `Executor` 契约：恰好一次调用 `run()`，不丢弃、不重复、不换线程 |
| U3 | MUST NOT 通过反射等方式依赖 package-private 内核类自建执行管道 |
| U4 | MUST NOT 期望上下文传播普通 `ThreadLocal` 或 MDC——传播集合封闭（A3） |

## 7. 不变量

| 编号 | 不变量 | 校验方式 |
|---|---|---|
| INV-1 | 上下文层位于所有用户装饰器之外（I1） | 顺序断言 + 装饰器内上下文可见 |
| INV-2 | 对装饰器同步同线程动态范围内的任务代码，回放生效（I2） | 装饰器内读上下文 == 提交线程值；逃逸负例 |
| INV-3 | 精确恢复：成功、异常、Error、中断、拒绝、inline 都恢复 worker 原状态 | 单线程池连续执行污染/失败/后继任务 |
| INV-4 | 顺序稳定：注册序 = 由外到内；每任务恰好一次 | 多装饰器 enter/exit 序列断言 |
| INV-5 | 快照独立：每次 prepare 独立捕获，不跨任务复用 | 并发提交不同上下文值 |
| INV-6 | 传播集合封闭：只传播登记的 `TransmittableThreadLocal` | 登记/未登记对照测试 |
| INV-7 | future 身份唯一：提交给 executor 与返回给调用方是同一实例 | spy executor 引用相等断言 |
| INV-8 | 归因不变：装饰器异常 → `USER_FAILURE`，不新增词汇 | 异常装饰器 → 批/组 outcome 断言 |
| INV-9 | 取消/deadline/fail-fast 语义与是否注册装饰器无关 | 带装饰器的取消/fail-fast/timeout 测试 |
| INV-10 | inline 路径与正常路径顺序相同 | 拒绝回退测试 |
| INV-11 | 库只用 `execute()`，且拒绝必终结 future | spy executor + 拒绝型 executor |
| INV-12 | 能力探测不改变身份 | `ExecutorIdentity` 仍 == 注册对象 |

## 8. 验证矩阵

| 编号 | 用例 | 断言 |
|---|---|---|
| T1 | 顺序 | 记录 enter/exit → `[context, scoped, d_N, …, d_1, body]` |
| T2 | 上下文可见性 | 装饰器内 TTL 可见、普通 `ThreadLocal` 不可见、`TaskExecutionContext.current()` 非空 |
| T3 | 三路径一致 | batch / group member / combine 各装饰一次，顺序相同 |
| T4 | 注册面 | 全局 + per-Par 同时注册 → 各应用一次，全局在外 |
| T5 | 异常 | 装饰器抛异常 → `USER_FAILURE`；listener 收到失败事件 |
| T6 | 取消 | 装饰器内 token 已取消时任务不进入用户代码；重试装饰器自查 token |
| T7 | 逃逸负例 | 装饰器把 `next` 交给另一线程 → 该段代码看不到快照（证明 I2 边界） |
| T8 | 快照隔离 | 并发提交不同上下文值 → 各任务看到各自快照 |
| T9 | 恢复精确性 | worker 预置非 null 值 → 任务结束后仍为该值 |
| T10 | inline | 拒绝回退路径顺序与正常路径相同 |
| T11 | executor 契约 | spy 断言调用的是 `execute` 而非 `submit`；拒绝型 executor → future 终态且无悬挂 |
| T12 | 重复调用外层 | 装饰器内重复调用外层上下文包装 → `IllegalStateException`（负例断言） |

## 9. 明确不做

| 方案 | 否决理由 |
|---|---|
| executor 包装 SPI | JDK 的 `ExecutorService` 已是扩展点；再加一层只会让身份、内省与上下文范围问题更隐蔽（P11–P13） |
| future 包装接缝 | 破坏 phase/purge/取消/恒等（P9）；future 级能力应进库扩展 |
| 关闭/替换上下文包装的开关 | 直接破 I1（P19） |
| 承诺覆盖装饰器异步逃逸的代码 | Java 8 无法阻止包装器换线程；只能靠契约 + 负例测试暴露（P6） |
| 承诺传播任意 `ThreadLocal`/MDC | 传播集合封闭（A3） |
| `WrapperChain`/`Wrapper` 组合类型 | `List<TaskDecorator>` 顺序应用已足够；新概念不消除任何一类错误（判据 2） |
| 可选的 Listening 包装 | `ListenableFuture` 是内部实现细节；对外契约是 `TaskBatchResult`/`TaskGroupResult` |
| `MultiTaskOptions.taskDecorator(...)` | 把行为塞进纯执行参数，且组内每个成员重复配置；按 Par 注册一次即可 |
| `TaskDecorator<T>` + 通配列表 | 堆污染（P14） |

## 10. 落地顺序

1. **L3 检测（可独立先行）**：`ExecutorRuntime` 加 `TtlUnwrap.isWrapper` 告警 + 能力探测解包 + 回归测试；
2. 本文档 + `design/AGENTS.md` 索引 + `CHANGELOG.md` 记录；
3. `TaskDecorator` SPI + `TaskSubmissions.wrapScoped` 参数化 + `GlobalPar` 注册面；
4. §8 验证矩阵；
5. 用户文档（中英）：顺序、捕获时点、逃逸禁令、重试语义、注册面追加语义。

## 11. 参考

- [first-principles.md](first-principles.md)：公理与评估判据
- [../docs/zh/design/idea-graveyard.md](../docs/zh/design/idea-graveyard.md)：否决记录
- [task-group-submission.md](task-group-submission.md) §7.3、§9：单任务提交内核的复用边界
- [task-group-lifecycle.md](task-group-lifecycle.md) §4.8：TTL 边界
- `internal/TaskSubmissions`、`internal/ScopedCallable`、`internal/ExecutionPhaseHintFuture`、`scope/ExecutorRuntime`、`scope/ExecutorIdentity`
- TTL 2.14.5：`TtlCallable`（`get` 的 idempotent 语义、`releaseTtlValueReferenceAfterCall`）、`TtlExecutors`（agent 短路）、`TtlUnwrap`
