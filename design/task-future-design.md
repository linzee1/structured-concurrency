# Task / TaskFuture 设计契约：forwarding 模式的增强 ListenableFuture

> 状态：**提案，待讨论**。前置分析见
> [enhanced-listenablefuture-analysis.md](enhanced-listenablefuture-analysis.md)
> （现状增强盘点与缺口）；本文是其结论的契约化：目标、约束、不变量、API、实现方式。
> 通过后沉淀进 task-group 契约系列与 user-guide；未通过前不提交、不实现。

## 1. 目标

1. **把库已有的内核语义就地下放到用户手里的 future 上**：任务名、终态归因、
   deadline 预算。不新增任何执行机制，只公开已有数据的只读视图。
2. **增强可传递**：库交付给用户的每一个"任务执行 future"都实现增强接口；
   内部流转（滑动窗口占位、桥接、组收敛）不丢增强。用户用 `instanceof` 检查接口
   即可调用，检查失败时退化为普通 `ListenableFuture` 用法，不会出错。
3. **不破坏 Guava 组合生态**：增强类型仍然是 `ListenableFuture`，
   `Futures.allAsList/transform/addCallback` 等全部照常工作（公理 4：不另造平行宇宙，
   只加只读仪表）。

### 非目标

- 链式编排（`thenApply`/`exceptionally` 等）——graveyard 已否决，不翻案。
- 暴露 `CancellationToken` 本体、phase 状态机、purge 信号——机制保持包私有。
- 替代 `TaskCompletion`/`TaskGroupResult` 快照通道——listener 事件与收敛快照
  仍是遥测与组级归因的权威来源。
- 用户自行构造增强 future（构造不公开；接口面向 `instanceof` 消费，不面向实现）。

## 2. 公开 API

```java
/**
 * 库交付的任务执行 future 的增强契约。
 * 用户以 instanceof 检查本接口；库保证其交付的每个任务 future 都实现它。
 */
public interface TaskFuture<T> extends ListenableFuture<T> {
    /** 任务名（批 = 批名；组成员/combine = 声明名；组完成 future = 组名）。 */
    String taskName();

    /**
     * 该任务视角的终态归因。未终态返回 RUNNING；一旦终态，任意时刻读取
     * 返回同一稳定值（不变量 I3，确定性论证见 §5.2）。取消归因按有序规则：
     * 直接取消（发起者）→ 内核显式归因 → 自身 deadline → 作用域 token。
     * 失败区分 USER_FAILURE 与 SUBMISSION_FAILURE。
     *
     * 视角约定：本方法与成员快照 TaskCompletion.outcome() 同为"成员视角"
     * （区分取消发起者与连带者），二者一致；TaskGroupResult.outcome() 是
     * "组收敛视角"，对同一事件可以给出不同答案（例：成员被直接取消时，
     * 成员视角 MEMBER_CANCELED，组视角 GROUP_CANCELED）——语义不同，非矛盾。
     */
    TaskOutcome outcome();

    /** 绝对 deadline（System.nanoTime 基准），与该任务的 CancellationToken 一致。 */
    long deadlineNanos();

    /** 距 deadline 的剩余预算；已过期返回 Duration.ZERO（不为负）。 */
    Duration remaining();

    /** USER_FAILURE / SUBMISSION_FAILURE 时的 cause；其余情况为 null。 */
    @Nullable Throwable failure();
}

/**
 * TaskFuture 的 forwarding 实现。构造不公开，实例只能来自库的交付点。
 */
public final class Task<T> extends ForwardingListenableFuture<T> implements TaskFuture<T> {
    // 包私有工厂；delegate 创建时固定（不变量 I2）
}
```

命名说明：接口是契约（用户只对它编程），类是载体。`Task` 无后缀，符合"它是
一次任务执行的句柄"的直觉；与 `TaskOutcome`/`TaskCompletion`/`TaskOptions` 同族。

**为什么不是 `extends FluentFuture`。** 两个硬事实（已对照 guava 33.6.0 源码核实）：

1. `FluentFuture` 的构造器是**包私有**（`FluentFuture() {}`，无访问修饰符），
   包外继承在技术上不成立；
2. 官方 javadoc 的 Extension 一节推荐的正是本方案——"declare your own subclass of
   `ListenableFuture`, complete with a method like `from()` to adapt an existing
   `ListenableFuture`, implemented atop a **`ForwardingListenableFuture`** that forwards
   to that future and adds the desired methods"。

`Task`/`TaskFuture` 与该推荐逐条对应：`TaskFuture` 即"ListenableFuture 的子接口"，
`Task` 即"ForwardingListenableFuture 实现"，包私有工厂（§5.1 的 `of`/`placeholder`）
即"from() 适配器"——适配只发生在内核交付点，因为归因数据（token、名字）只有内核有，
这与"用户不能构造"的非目标一致。

**fluent 链不复制。** `transform`/`catching`/`withTimeout` 等链式方法不进接口；
需要时 `FluentFuture.from(task).transform(...)`（`from` 对已是 FluentFuture 的实例
零包装）。链上派生的 future 是普通 `FluentFuture`，**增强不流入用户编排链**——
派生 future 不是库执行的任务，没有可归因的 token；这与
["编排交给 Guava/CF"](../docs/zh/design/idea-graveyard.md) 的否决记录一致。

返回类型变更（声明收窄，源码兼容、二进制不兼容，0.x 可接受）：

| 交付点 | 现状返回 | 新返回 |
|---|---|---|
| `TaskBatchResult.results()` 元素 | `ListenableFuture<T>` | `TaskFuture<T>` |
| `TaskGroup.future(key)` / `members()` / `findMember()` | `ListenableFuture` | `TaskFuture<T>` / `TaskFuture<?>` |
| `TaskGroup.completionFuture()` | `ListenableFuture<TaskGroupResult>` | `TaskFuture<TaskGroupResult>` |
| （v0.3 提案）`Builder.task()/combine()` | — | `TaskFuture<T>` |
| `TaskBatchResult.submitCanceller()` | `ListenableFuture<?>` | **不变**（控制句柄，不代表任务执行，见约束 C6） |

## 3. 约束

- **C1 接口即契约。** 公开文档只承诺 `TaskFuture` 接口；`Task` 类公开但构造不公开
  （包私有工厂），用户 MUST NOT 依赖具体类。用户侧标准用法：
  `if (f instanceof TaskFuture) { ((TaskFuture<?>) f).outcome(); }`。
- **C2 访问器全时安全。** 接口全部方法：任意线程、任意生命周期点可调用、非阻塞、
  幂等、不抛受检异常。`outcome()` 在取消/失败竞态下也必须返回合法枚举值（见 I3）。
- **C3 词汇复用。** 归因只用现有 `TaskOutcome` 枚举；时间基准沿用
  `System.nanoTime`（与 `CancellationToken.deadlineNanos()` 一致）；不引入新枚举、
  新异常类型。
- **C4 零新依赖、Java 8。** `ForwardingListenableFuture` 来自现有 Guava 依赖。
- **C5 代码风格。** 访问器 `x()` 风格；`@ParametersAreNonnullByDefault` +
  `javax.annotation.Nullable`（public API）；JUL 日志。
- **C6 只包装任务执行 future。** `Task` 包装的对象必须代表一次任务执行（批元素、
  组成员、combine、组收敛）。控制句柄（`submitCanceller`、token 内部组合 future、
  completion queue 内部引用）保持裸 future，不包装。
- **C7 委托透明。** `get/isDone/isCancelled/cancel/addListener` 的语义与超时、
  中断行为完全等于直接操作 delegate；`Task` 不改变任何 Guava future 语义，
  只追加只读信息。`cancel(mayInterruptIfRunning)` 原样透传。
- **C8 不继承 `FluentFuture`。** 遵循官方 Extension 推荐（子接口 +
  `ForwardingListenableFuture` 实现 + `from()` 适配）；且其构造器包私有，
  继承在技术上不成立。fluent 能力经 `FluentFuture.from(task)` 获得，
  不在本类型上复制链式 API。

## 4. 不变量

- **I1 交付即增强。** 库返回给用户的每一个任务执行 future（约束 C6 范围内）
  `instanceof TaskFuture` 恒为真——窗口内外、占位与真实、成功与失败路径无例外。
  这是"用户可以放心 instanceof"的根基；`PublicApiSurfaceTest` + 专项测试守护。
- **I2 delegate 创建时固定，永不交换。** 占位场景下 `Task` 包装的是
  `SettableFuture` 本体，桥接仍由 `placeholder.setFuture(real)` 完成——
  因为桥接前用户可能已 `addListener` 或 `cancel` 占位，这些登记和取消都落在
  占位本体上；交换 delegate 会丢掉它们。Guava `setFuture` 语义保证：
  桥接后占位跟随真实 future 终态；占位先被取消则 `setFuture` 返回 false 并级联
  `cancel(true)` 真实 future（cancellation-propagation §4）。forwarding 模式下
  这一切原样保留，零重实现。
- **I3 归因确定。** `outcome()`：未终态 → `RUNNING`；一旦终态，任意时刻读取
  返回同一值。注意这不是"token 先 CAS"一条就能保证的：组可以**因成员的取消
  而随后取消**（member cancel → `groupToken.cancel()` → 传播把 member token
  改写为 PROPAGATED_CANCELED），活读 token 会随读取时刻漂移（实测冲突，
  2026-09 修订）；冻结 listener 则有注册顺序脆弱性。确定性由 §5.2 有序规则的
  四个输入合成，论证：token 状态机单调（RUNNING 只出度一次 CAS）；级联路径下
  责任 token 在取消动作前已 CAS（现有 token 不变量）；`directCancel` 置旗先于
  转发（I6）；`kernelOutcome` 在占位完成前写入。future 终态时仍为 RUNNING 的
  token 必然伴随终态前已写入的短路记录（①或②），故四个输入在终态时全部定型，
  任意后置计算结果相同。竞态细则：`TokenOutcomes.causedByCancellation` 识别的
  "checkpoint 异常赢了级联竞态"按取消归因，不按 `USER_FAILURE`；两个独立原因
  的真竞态（用户取消与 deadline 同时）允许任一结果——保证的是终态后稳定，
  而非竞态输赢。
- **I4 身份随壳不随芯。** `taskName()`、token 引用（outcome/deadline 的数据源）
  存在 `Task` 壳上，创建时赋值，与 delegate 的生命周期解耦：窗口外占位从创建起
  就有名字和归因能力，桥接、abandon 都不改变它们。这修复了现状"占位符无归属信息"
  （`SlidingWindowSubmitter` L187-191 的无归因终态）。scopeToken 同样在创建时
  可得（组 token 先于成员 future 创建），无需回填。
- **I5 引擎对象不换。** 提交给 executor 的仍然是 `ExecutionPhaseHintFuture` 本体
  （purge 观察、精确中断、inline fallback 全部不受影响）；`Task` 只是它的用户侧
  视图，绝不进入执行管道。内核需要引擎能力时持有裸引用，不经过壳。
- **I6 取消级联绕过壳。** `CancellationToken.bind` 的输入列表、组 cancel、
  fail-fast 级联一律作用于引擎 future / 占位本体（`Task` 暴露包私有
  `kernelDelegate()`），绝不调用 `Task.cancel()`。因此壳上的 `cancel()` 有且
  仅有用户直接取消一个触发源——`directCancel` 标志据此区分"发起者"与
  "连带者"，这是 §5.2 规则①成立、进而 I3 成立的根基。

## 5. 实现方式

### 5.1 `Task` 的结构

```java
public final class Task<T> extends ForwardingListenableFuture<T> implements TaskFuture<T> {
    private final ListenableFuture<T> delegate;        // I2：final，创建时固定
    private final String taskName;
    private final CancellationToken token;             // outcome/deadline 数据源
    @Nullable private final SettableFuture<T> placeholder;  // 占位场景非 null，供 bind

    @Override protected ListenableFuture<T> delegate() { return delegate; }
    // taskName()/outcome()/deadlineNanos()/remaining()/failure() 见 5.2
    // 包私有 void bind(ListenableFuture<T> real)：placeholder.setFuture(real)，仅占位可调用
}
```

工厂遵循官方 `from()` 适配习语但仅内核可见：`Task.of(name, token, delegate)` 包装
真实执行 future；`Task.placeholder(name, token)` 自建 `SettableFuture` 占位并包装。
两者是仅有的创建入口（I1 的守护点）。

两类实例，同一类型：

| 场景 | delegate | placeholder | 创建点 |
|---|---|---|---|
| 窗口内批元素 / 组成员 / combine | `ExecutionPhaseHintFuture` 本体 | null | `TaskSubmissions.prepare` 出参处包装（prepare 已持有 name/context/token，零成本；需把 context→token 引用多传一根） |
| 窗口外批占位 / （v0.3）声明期占位 | `SettableFuture.create()` | 同一对象 | `SlidingWindowSubmitter.submitAll` L88、v0.3 `Builder.task()` |
| 组完成 future | `SettableFuture<TaskGroupResult>` | 同一对象 | `TaskGroup` L60，token = 组 token，taskName = 组名 |
| 初始提交失败的批 | `Futures.immediateFailedFuture`（包 `SubmissionException`） | null | `SlidingWindowSubmitter` L73-76 |

### 5.2 访问器实现

- `outcome()` = `FutureInspector` 逻辑上移：done 且非 cancelled → 成功 SUCCESS /
  失败按 `SubmissionException` 区分 SUBMISSION_FAILURE·USER_FAILURE；cancelled →
  `TokenOutcomes.forCanceled(token.state()/originState())`；未完成 → RUNNING。
  上移后 `FutureInspector` 对接口编程，`instanceof ExecutionPhaseHintFuture`
  分支退役（天然迁移接缝）。
- `deadlineNanos()/remaining()` 直接转发 `token.deadlineNanos()/remaining()`，
  `remaining()` 负值钳到 ZERO。
- `failure()`：终态失败时取 `FutureInspector.exceptionNow` 的 cause；其余 null。
- `toString()`：`Task[name=get-orders, state=RUNNING, deadline=…]` 含占位桥接状态
  （v0.3"忘记 submit"诊断也靠它）。

### 5.3 内核改造点（全清单）

1. `TaskSubmissions.prepare`：返回值或出参携带包装后的 `Task`（context→token
   引用一根新线）。
2. `SlidingWindowSubmitter`：占位创建改为 `Task` 包装；L161/L187 两处
   `(SettableFuture<V>)` 向下强转改为 `task.bind(real)` / `task.abandon(...)`
   自述方法；abandon 的提交失败路径统一包 `SubmissionException` 以保证
   `SUBMISSION_FAILURE` 归因（I4）。
3. `TaskGroup`：成员/combine future 包装（名字 = TaskKey 名，token = 成员 token）；
   completionFuture 包装（名字 = 组名，token = 组 token）。
4. `FutureInspector`：改为对 `TaskFuture` 接口编程；保留对裸 future 的保守
   fallback（兼容 `submitCanceller` 等未包装句柄经过的代码路径）。
5. `TaskBatchResult.results()`、`TaskGroup` 成员访问器：声明类型收窄为
   `TaskFuture`；`CancellationToken.bind` 等组合点不变（它们接受任意 LF）。
6. `PublicApiSurfaceTest` 白名单 +2（`TaskFuture`、`Task`）。

### 5.4 与 equals/hashCode/toString

Guava `ForwardingObject` 不转发 `equals/hashCode/toString`。规定：
`equals/hashCode` 保持对象身份语义（future 在实践中按身份比较，文档明示）；
`toString` 必须重写为诊断串（5.2）。

## 6. 能力取舍：哪些功能放在 LF 上

按"消除哪类误用 + 数据是否创建点在手"逐项判定：

| 候选 | 判定 | 理由 |
|---|---|---|
| `taskName()` | **放** | `Checkpoints.checkpoint(taskName,…)` 隐式约定可自检；诊断/日志免外部映射 |
| `outcome()` | **放（核心）** | 消除"用 `isCancelled()` 事后猜原因"整类误用；归因表与 token 顺序保证已就绪 |
| `deadlineNanos()/remaining()` | **放** | 嵌套任务预算分配的唯一官方读数；token 现成 |
| `failure()` | **放** | 与 `outcome()` 配套的失败详情口；免去 `get()` 拆 `ExecutionException` |
| `toString` 诊断 | **放**（类约定，不进接口） | 占位未桥接/未提交状态的主要诊断面 |
| `executorName()`（执行器标签） | **暂缓** | 数据在手（`MultiTaskContext.executorLabel()`），但只服务图诊断；等首个真实需求再加，加是兼容的 |
| 时间戳/计时（submit/start/end、wait/execution） | **不放** | 与 `TaskCompletion` 快照通道重复；放上 future 会诱导绕过 listener 体系，两个通道语义漂移 |
| `taskIndex()/unitId()/groupId()` | **不放** | index 可由 `results()` 位置推导；groupId 需回填（组对象晚于成员 future 创建）；均为遥测，归 `TaskCompletion` |
| `checkpoint()` 等协作取消 | **不放** | checkpoint 是执行线程的线程上下文行为（`TaskExecutionContext.current()`），future 在提交者手里，通道不对 |
| `cancel(reason)` 带归因取消 | **不放** | 成员级取消归因已由 token 直读得出；开放原因参数会让归因词汇失控 |
| fluent 链（transform/catching/withTimeout） | **不复制**，经 `FluentFuture.from(task)` 获得 | `FluentFuture` 构造器包私有不可继承（C8）；官方推荐 forwarding 模式；派生 future 是普通 `FluentFuture`，增强不流入用户编排链 |
| 链式编排 | **不放** | graveyard 否决项 |
| 暴露 token/phase/purge 信号 | **不放** | 机制包私有边界不动；future 只给派生只读视图 |

原则：**接口每加一个方法都要过"消除哪类误用"的检验**；宁可后续兼容地加，
不要一开始长成第二个 `TaskCompletion`。

## 7. 兼容与迁移

- 声明返回类型收窄：`ListenableFuture<T>` → `TaskFuture<T>`。赋值给
  `ListenableFuture<T>` 的调用方源码兼容；二进制不兼容（0.x 接受，写入
  migration-v0.3）。
- `TaskFuture` 继承 `ListenableFuture`，所有 Guava 组合 API 不受影响。
- 用户侧零迁移成本：不检查接口的代码行为完全不变。

## 8. 测试矩阵

1. **I1 交付完整性**：批（窗口内/窗口外/初始失败）、组成员、combine、
   completionFuture 全部 `instanceof TaskFuture`。
2. **归因全枚举**：`outcome()` 覆盖 SUCCESS / USER_FAILURE / SUBMISSION_FAILURE /
   MEMBER_CANCELED / GROUP_CANCELED / FAIL_FAST / TIMEOUT / RUNNING；含
   `originState()` 传播链（嵌套批/组）。
3. **竞态**：cancel 与 failure 竞态、checkpoint 异常赢级联竞态
   （`causedByCancellation`）、deadline 与成功同时；I3 稳定性（终态后多次读一致）。
4. **占位桥接**：桥接前 `addListener` 的 listener 桥接后触发；桥接前 cancel →
   真实 future 桥接时被级联取消；abandon 两路径（中断/拒绝）归因正确。
5. **委托透明**：`get` 结果/异常、`cancel(true)` 中断 worker、
   `addListener(executor)` 语义与 delegate 一致。
6. **预算**：`remaining()` 单调不增、过期钳 ZERO、`deadlineNanos()` 与 token 一致。
7. **组完成 future**：组名、组 token 归因、正常完成时 `outcome()==SUCCESS`。

## 9. 与 v0.3 group 提案的关系

本文是 v0.3 的**前置**：v0.3 的"声明期返回占位 future"依赖本文的占位壳
（I4：名字与归因随壳）；v0.3 实施时 `Builder.task()/combine()` 直接返回
`TaskFuture<T>`。落地顺序：**Task/TaskFuture → group v0.3**。
