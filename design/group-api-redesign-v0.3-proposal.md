# TaskGroup 用户接口重新设计（v0.3 提案）

> 状态：**提案，待讨论**。本文件不是契约；通过后其结论应沉淀进 `task-group-*.md`
> 契约系列与迁移指南，本文件随后删除或归档。
>
> 出发点：用户反馈"group 并行处理多个任务的接口功能不内聚、依赖过多、引入了多余的概念"。
> 本文从 [first-principles.md](first-principles.md) 的公理重新推导最小用户接口，
> 刻意不从现有 API 的形状出发。执行内核（取消传播、deadline、TTL、统一提交）完全不变，
> 本文只改用户可见表面。

## 1. 问题诊断：概念清点

用户只想表达一件事：**"这几个不同类型的任务，放一起并行跑，共用一个 deadline，
任何一个失败就全部取消，我要类型安全地拿到每个结果。"**

现状（v0.2）为这一件事必须接触的类型（group 路径）：

| # | 类型 | 用户感知到的职责 |
|---|---|---|
| 1 | `TaskGroupOptions` | 组名 + timeout 二选一工厂 |
| 2 | `TaskGroupDefinition` | 纯数据描述（不可变、可复用） |
| 3 | `TaskGroupDefinition.Builder` | 收集成员 |
| 4 | `TaskKey<T>` | 匿名子类捕获结果类型 + 携带名字 |
| 5 | `ParName` | 按名引用 executor（使用点） |
| 6 | `TaskOptions` | 成员级 timeout/taskType/rejectEnqueue |
| 7 | `TaskGroup` | submit 后的运行句柄 |
| 8 | `TaskGroupResult` | 终态快照 |
| 9 | `TaskOutcome` | 终态词汇 |
| 10 | `TaskCompletion` | 成员完成记录（listener 事件与快照双役） |
| 11 | `CombineFunction<R>` | 终端汇合函数 |
| 12 | `CompletedTaskValues` | combine 的取值视图 |
| 13 | `TaskGroupListener` | 组级监听 SPI |
| 14 | `TaskGroupListener.TaskGroupEvent` | `TaskGroupResult` 的薄包装 |

具体症状：

- **同一个名字声明两次、两种载体**：`new TaskKey<List<Order>>("get-orders") {}` 里的
  名字是 key 的成员；`ParName.of("order")` 是值对象；而批/组名是裸 `String`。
  一个库里"name"有三种形态。
- **为"取回类型安全的 future"专门造了一个类型系统**：`TaskKey` 匿名子类 +
  `future(key)` + `findMember(String)` + `members()` 三条取路并存的唯一原因，
  是"future 只在 submit 后才存在"这条自我施加的约束。而内核已经握有破解它的机制
  （`SlidingWindowSubmitter` 的 `SettableFuture` 占位符 + `setFuture()` 桥接）。
- **选项三件套中两个是仪式的**：组只有 name/timeout/listener 三个字段，
  却要求 `TaskGroupOptions.timeout(...)` 工厂 + builder 入口参数两级包装；
  成员 `TaskOptions` 绝大多数调用点省略（等价 `inheritTimeout()`）。
- **listener 与 future 重复**：`completionFuture().addListener(...)` 已经能表达
  组完成回调，`TaskGroupListener`/`TaskGroupEvent` 是第二套平行的通知机制
  （公理 4：不另造平行宇宙）。
- **definition 与 group 的分离服务的是一个未被验证的需求**："definition 纯数据、
  可换拓扑复用、可重复提交"是推测性通用化；它直接导致了 ParName 字符串引用
  （submit 时才解析、才可能失败）和 TaskKey 令牌模型两个笨重设计。

## 2. 从公理重新推导

对照 first-principles 的四条公理与六条判据：

1. **结构化并发是本体** → 必须有一个可关闭的组作用域对象：保留 `TaskGroup`
   （submit 后句柄 + `close()` 兜底取消）。这条不变。
2. **用户出错的方式是"忘记"** → 评测每个概念："它消除了哪类忘记？"
   - `TaskKey`：不消除任何忘记，反而引入"key 名与 callable 不匹配"的新错法。**删。**
   - `TaskGroupOptions`：timeout 强制二选一值得保留（显式选择逼用户思考），
     但不需要一个类型来承载——两个静态工厂参数即可表达。**折叠。**
   - `TaskGroupListener`：不消除忘记，与 future listener 重复。**删。**
   - `CombineFunction`/`CompletedTaskValues`：取值可以用成员 future 自己表达
     （combine 运行时成员已成功，`future.get()` 非阻塞）。**合并为普通 `Callable`。**
3. **安全性优先于表达力** → definition 的"可复用/可换拓扑"是表达力，
   它购买的不是安全而是猜测中的便利。**舍弃复用，换单次使用的内聚声明。**
4. **贴近 JDK 习语** → 目标心智模型是 `invokeAll` / JDK 21 `StructuredTaskScope`
   的 fork-join：声明任务 → 拿回 future → 等全部完成。API 形状应向它收敛，
   而不是向"配置对象 + 构建器 + 提交器"三件套收敛。

机制层面的关键解锁（判据 2：用现有机制参数化表达，而不是新增概念）：

> **成员 future 在声明时就可以创建。** 内核的不变量 already 是：窗口外任务返回
> `SettableFuture` 占位符，槽位释放时 `setFuture()` 桥接。把同一招用到 group 声明期：
> `task(...)` 立即返回一个占位 `ListenableFuture<T>`，`submit()` 统一冻结时桥接到
> 真实执行 future。于是"类型化取回"不再需要任何 key 类型——泛型方法返回值天然携带类型。

由此得到 group 路径的最小概念集（6 个，其中 4 个与 batch 路径共用）：

| 概念 | 角色 | 备注 |
|---|---|---|
| `TaskGroup` | 组作用域（声明期收集 + submit 冻结 + 运行句柄 + close 取消） | 内嵌 `Builder` 承担声明期 |
| `TaskOptions` | 单次任务执行的可选微调（timeout/taskType/rejectEnqueue） | 与现状相同，batch 不共用 |
| `TaskGroupResult` | 终态快照（outcome/failedTaskName/members/时间戳） | 不变 |
| `TaskOutcome` | 全库统一终态词汇 | 不变 |
| `TaskCompletion` | 成员完成记录（listener 事件 + 快照） | 不变 |
| `ListenableFuture<T>` | 成员结果的**唯一**载体 | Guava 原生语义，零新概念 |

## 3. 目标 API

### 3.1 用户代码（全部）

```java
// 装配：注册 executor 时直接拿回 Par（register 返回 Par，或 global.par("user")）
GlobalPar global = GlobalPar.builder()
        .register("user", userPool)
        .register("order", orderPool)
        .build();
Par userPar = global.par("user");
Par orderPar = global.par("order");

// 声明：组名与 timeout 的强制二选一由两个工厂表达，没有 options 类型
TaskGroup.Builder tasks = TaskGroup.builder(global, "account-page", Duration.ofSeconds(3));
// 嵌套场景改用 TaskGroup.inheriting(global, "account-page")，语义同今 inheritTimeout

// 声明即拿回类型化 future；名字只出现一次；executor 传 Par 对象，编译期类型安全
ListenableFuture<User> user = tasks.task("get-user", userPar, userService::getUser);
ListenableFuture<List<Order>> orders = tasks.task("get-orders", orderPar, orderService::getOrders,
        TaskOptions.timeout(Duration.ofSeconds(1)).taskType(TaskType.IO_BOUND));

// 可选：终端汇合。就是普通 Callable——成员已全部成功，get() 非阻塞
ListenableFuture<Page> page = tasks.combine(pagePar,
        () -> buildPage(user.get(), orders.get()));

// 提交：唯一的冻结边界（不变量不变）
try (TaskGroup group = tasks.submit()) {
    User u = user.get();                              // 普通 Guava future 语义
    TaskGroupResult result = group.completionFuture().get();  // 归因快照（数据，非异常）
}                                                     // close() = 未提交/未完成时兜底取消
```

对比现状：14 个类型 → 6 个；用户写出的 import 从 9 个降到 4 个
（`GlobalPar`/`Par`/`TaskGroup`/`TaskOptions`）。

### 3.2 公开签名

```java
public final class TaskGroup implements AutoCloseable {

    public static Builder builder(GlobalPar global, String name, Duration timeout);
    public static Builder inheriting(GlobalPar global, String name);

    // —— submit 后的运行句柄（同现状）——
    public String groupId();
    public String groupName();
    public void cancel();                                    // 幂等、非阻塞
    public ListenableFuture<TaskGroupResult> completionFuture();
    public Map<String, ListenableFuture<?>> members();       // 唯一的成员枚举口，观测用
    @Override public void close();                           // 未完成 ⇒ 等同 cancel()

    public static final class Builder {
        public <T> ListenableFuture<T> task(String name, Par par, Callable<T> callable);
        public <T> ListenableFuture<T> task(String name, Par par, Callable<T> callable,
                                            TaskOptions options);
        public <R> ListenableFuture<R> combine(Par par, Callable<R> callable);        // 可选，至多一个
        public <R> ListenableFuture<R> combine(Par par, Callable<R> callable, TaskOptions options);
        public TaskGroup submit();                           // 唯一冻结点；Builder 单次使用
    }
}
```

连带简化（不属于 group 本体，但同源冗余）：

- `GlobalPar.Builder.register(String, ExecutorService)` 返回注册好的 `Par`；
  `GlobalPar.par(String)` 接受裸字符串。**`ParName` 从公开 API 删除**（值对象包装的
  只是一个校验过的 String，按名查表的场景没有获得任何类型安全收益）。
- `TaskGroupListener`/`TaskGroupEvent` **删除**：组完成通知统一走
  `completionFuture().addListener(...)`（Guava 习语，强制指定回调 executor）。
- `Par.map` 与 `BatchOptions` 本提案不动；组名/批名统一为裸 `String`，
  name 校验收敛到一处工具方法。

### 3.3 语义（相对 v0.2 不变的性质，逐条对应契约）

- 统一提交边界不变：`submit()` 仍按提交线程解析结构父与 observation，
  全部成员准备完成后才允许任何成员进入 executor；executor rejection 走统一 admission
  失败路径（见 task-group-submission.md），此时全部占位 future 以 `SUBMISSION_FAILURE` 完成。
- 取消拓扑、fail-fast 级联、deadline `min(自己, 父)`、TTL 快照回放、`originState()`
  归因全部不变——它们都在内核，本提案不触碰。
- 成员 future 保持普通 Guava 语义：成功返回值、失败 `ExecutionException`、取消即
  cancelled。占位符桥接用 `SettableFuture.setFuture()`，与滑动窗口同一机制。
- `combine` 维持现有语义：至多一个、仅在全体成员成功后运行、受组 deadline 覆盖；
  变化仅在签名（`Callable<R>` 取代 `CombineFunction<R>` + `CompletedTaskValues`），
  以及声明位置自由（不再要求终结调用，因为它读的是 future 而非注册表）。
- `TaskGroupResult`/`TaskOutcome`/`TaskCompletion` 结构不变；成员快照仍按声明序。

### 3.4 新增的失败模式与对策（诚实清单）

| 失败模式 | 现状如何避免 | 新设计的行为 | 对策 |
|---|---|---|---|
| 忘记 `submit()` 就 `future.get()` | 不可能（拿不到 future） | 占位 future 永不完成，挂起 | 文档明示；`close()` 未提交时以 `LeanCancellationException` 失败全部占位符；占位符 `toString` 携带 "not submitted" 诊断 |
| `Builder` 重复 `submit()` | definition 可重复提交（特性） | 第二次 `submit()` 抛 `IllegalStateException` | 单次使用写进契约；需要重复执行就包一个方法重新声明 |
| 声明后丢弃 Builder 变量 | 同左 | 同"忘记 submit" | try-with-resources 模式是唯一推荐用法 |
| 成员 `Par` 不属于该 `GlobalPar` | submit 时按名解析失败 | 声明期即可校验 `par.globalPar() == global` | 反而把一类运行期错误提前到配置期 |

"忘记 submit 挂起"与 Guava `SettableFuture` 永不 set 是同一类已知模式，
以诊断 + close 兜底换取删除整个 `TaskKey` 类型系统，值得。

## 4. 旧 → 新概念映射

| v0.2 | v0.3 | 处置理由 |
|---|---|---|
| `TaskGroupOptions` | 工厂参数 `builder(global, name, timeout)` / `inheriting(global, name)` | 3 字段不配拥有一个类型；timeout 二选一仍由两个工厂强制 |
| `TaskGroupDefinition` + `.Builder` | `TaskGroup.Builder`（单次使用） | "可复用纯数据"是推测性通用化；复用需求用 Java 方法包装即可 |
| `TaskKey<T>` | **删除** | 占位 future 让类型随返回值流动；名字只在 `task(name, ...)` 出现一次 |
| `future(key)` / `findMember(String)` | **删除**，只留 `members()` | 声明时拿 future 后，运行时按键取回没有存在理由 |
| `ParName`（使用点） | 直接传 `Par`；`ParName` 整体移出公开 API | 字符串解析失败从 submit 提前到声明；少一次 `ParName.of()` 仪式 |
| `CombineFunction<R>` + `CompletedTaskValues` | 普通 `Callable<R>` | 成员 future 即取值视图；combine 运行时 `get()` 非阻塞 |
| `TaskGroupListener` + `TaskGroupEvent` | `completionFuture().addListener()` | 删除平行通知机制 |
| `TaskOptions` / `TaskGroupResult` / `TaskOutcome` / `TaskCompletion` / `TaskType` | 保留不变 | 各自通过了"消除哪类忘记"的检验 |
| `CancellationToken`（自助 API） | 保留不变 | 与 group 表面正交 |

公开顶层类型净变化：−6（删 `TaskGroupDefinition`、`TaskKey`、`ParName`、
`CombineFunction`、`CompletedTaskValues`、`TaskGroupListener`），
`TaskGroupOptions` 并入 `TaskGroup` 工厂。`PublicApiSurfaceTest` 白名单随实现更新。

## 5. 明确不做的事（对照否决记录）

- 不加 `failFast(false)`、重试、优先级、链式编排——graveyard 已否决，本提案不翻案。
- 不把 group 退化成 `List<Callable>` + 位置索引取结果（丢失泛型，正是 `TaskKey`
  当年要解决的问题；占位 future 用零类型成本解决了它）。
- 不引入"当前 group"隐式 ThreadLocal；结构父解析仍在 submit 现场。
- `Par.map`/`BatchOptions` 不在本提案范围；若未来追求一致性，可另行评估
  `Par.map(list, fn)` 的 name/timeout 是否也提升为工厂参数。

## 6. 实施概要（提案通过后）

1. `TaskGroup` 增加两个静态工厂 + 内嵌 `Builder`；声明期创建 `SettableFuture`
   占位符，submit 时桥接（复用 `TaskSubmissions` 两阶段内核，不复制取消/TTL 逻辑）。
2. 删除 6 个公开类型；`GlobalPar.Builder.register` 改为返回 `Par`、
   `GlobalPar.par(String)` 重载；name 校验收敛。
3. 测试：占位符桥接（成功/失败/取消/submit 前 close）、统一 admission 失败时
   占位符归因、Builder 单次使用、par 归属校验、原有取消/deadline/TTL 矩阵全量回归。
4. 文档：`docs/*/user-guide.md` 重写 group 章节、新增 `migration-v0.3.md`、
   更新本目录契约系列与 `PublicApiSurfaceTest`。
5. 按仓库约定：API 破坏性变更与实现、测试、用户文档、迁移笔记作为一次变更提交。
