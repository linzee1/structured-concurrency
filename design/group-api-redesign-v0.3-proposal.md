# TaskGroup 用户表面重做（v0.3）设计分析

> 状态：**提案草案，待对齐**。本文是设计分析，不是实现计划；落地前先与用户对齐 §9 的决策点。
> 已随讨论入库存档，但方向未定，不作为实现依据；后续修订继续在本文件上进行。
> 素材来源：现分支 `dev/v0.3.0` 代码与契约文档（全部结论带 `path:line`），
> `reports/defect-analysis-2026-09-10.md`，`design/first-principles.md` 判据。

## 1. 背景与目标

用户痛点：group 接口"功能不内聚、依赖过多、多余概念"。v0.3 Phase 1 已交付
`TaskFuture<T>`/`Task<T>`（`Task.java`、`TaskFuture.java`），Phase 2（本提案）重做
`TaskGroup` 的声明与交付形态：

```java
TaskGroup.Builder tasks = TaskGroup.builder(global, "account-page", Duration.ofSeconds(3));
// 嵌套场景：TaskGroup.inheriting(global, "account-page")

TaskFuture<User> user = tasks.task("get-user", userPar, userService::getUser);
TaskFuture<List<Order>> orders = tasks.task("get-orders", orderPar, orderService::getOrders, options);
TaskFuture<Page> page = tasks.combine("build-page", pagePar,
        () -> buildPage(user.get(), orders.get()));

try (TaskGroup group = tasks.submit()) {          // 唯一冻结点，Builder 单次使用
    User u = user.get();                          // 普通 Guava future 语义
    TaskGroupResult result = group.completionFuture().get();
}                                                 // close(): 未完成 ⇒ 等同 cancel()
```

目标：公开顶层类型净删 7 个（清单见 §8），内核零改动（取消拓扑、fail-fast、
deadline `min(自己,父)`、TTL 快照、`originState()` 归因全部保持）。

## 2. 焦点问题 2：第一性原理评估

对照 `design/first-principles.md` §四的六条判据：

1. **消除了哪类"忘记"？** 现行形态要用户记住三件事：定义时用 `TaskKey` 声明、
   取结果时凭同一个 key 回查、键名与定义名保持一致。新形态里**类型随返回值流动**
   （`tasks.task(...)` 直接返回 `TaskFuture<T>`），回查步骤与令牌概念整体消失；
   "忘记传对 key / key 类型与任务不匹配"这一类错误在编译期就不可能发生。
2. **能否参数化现有机制？** 能。声明期占位 = `SlidingWindowSubmitter` 窗口外占位
   （`SettableFuture` + `setFuture` 桥接，`Task.java:82-117`、
   `SlidingWindowSubmitter.java:90-92,167`）的同一招；submit 冻结、token 创建、
   两阶段 prepare→submit 全部沿用现有内核（`TaskGroup.java:487-508` 与
   `:602-604` 本就分离）。新增的只是"壳"，不是第二条执行管道。
3. **结构化语义是否自动继承？** 是——token/deadline/TTL 仍在 submit 现场解析
   （`TaskGroup.buildWhileOpen`，`TaskGroup.java:455-549`），声明期只捕获
   名称/Par/Callable 纯数据，与 `TaskGroupDefinition` 当前的"配置期无上下文"
   地位相同（`TaskGroupDefinition.java:11-18`）。
4. **是否引入隐式状态？** 否。Builder 是显式对象，不引入 current-group
   ThreadLocal；组完成通知从 `TaskGroupListener` 改为
   `completionFuture().addListener(...)` 是**贴近 Guava 习语**（判据 4 的
   正向），不是新增概念。
5. **失败形状能否用现有 outcome 词汇表达？** 能，无需新增 `TaskOutcome`。
6. **是否撑破抽象边界？** 否。combine 仍是"组内单一终端全量 join"，不是用户
   拓扑 DAG——`idea-graveyard.md` 否决的是编排/DAG（L43-66、L203-227），
   `task-group-terminal-combine.md:177-179` §10 已为此留好例外论证。

**唯一需要显式论证的减法**：删 `TaskKey` 后，graveyard L203-227 否决
`invokeAll` 时依赖的"类型安全"理由改由**返回值泛型**承担——类型安全没有丢，
承载者从令牌换成返回值，且更强（编译期绑定在声明点，不需要回查时对齐）。

## 3. 焦点问题 3：现在为什么不够好

1. **令牌模型重复持有名字。** `TaskKey` 的 name 与 `TaskDefinition` 的 name 是
   同一个名字的两次声明；`ParName` 与注册时的字符串同理。名字被包成对象后，
   相等性、校验、泛型捕获各需要一层类型（`TaskKey.java:57-64` 的 equals 按
   name、`TaskGroup.future(TaskKey)` L123-137 的运行期类型再校验）——这些机制
   在返回值泛型下全部是多余的。
2. **交付点不内聚。** 用户在定义侧（`TaskGroupDefinition.Builder`）声明、在
   运行侧（`TaskGroup.future(key)`）取回，同一条任务的身份被劈成两段；
   `CompletedTaskValues.value(key)`（`CompletedTaskValues.java:38-58`）是第三段。
   combine 读兄弟结果必须穿过 `CombineFunction`+`CompletedTaskValues` 两个只为
   它存在的类型，而它想做的事就是 `future.get()`。
3. **`TaskGroupOptions` 是 3 字段的过渡载体**：name + timeout + listeners。删
   listener 通道后只剩 name + timeout，折叠成两个工厂参数即可
   （`TaskGroupOptions.java:35-67`）。
4. **`TaskGroupListener` 与 future 监听重复。** 组级一次性回调通道存在的原因是
   "执行前取消/提交失败的成员也必须在组级可见"
   （`task-group-observability-and-verification.md:18`）——但 `TaskGroupResult`
   本就携带全成员不可变快照，`completionFuture().addListener` 提供完全等价的
   数据与时序（future 在收敛后完成，监听器必然在结果固定后触发）。
5. **概念计数**：group 相关公开顶层类型现为 14 个，其中 7 个只为"声明与回查"
   的间接层服务（§8）。

## 4. 焦点问题 1：更好的实现方法

### 4.1 声明期占位：三条路径的比较

| 路径 | 做法 | 判定 |
|---|---|---|
| (a) `Task` token 晚绑定 | `token` 字段去 final，submit 时 attach 一次 | 不推荐：破坏 `Task` "身份从创建起即最终值"的不变量（`Task.java:22-26`），`outcome()`/`deadlineNanos()` 无条件解引用 token（`Task.java:143,166,173`），全类要加 null 窗口防护 |
| (b) **独立声明期壳** | 新的包私有 `TaskFuture` 实现：声明期持 name + `SettableFuture`；submit 时 `setFuture(realTask)` 桥接并把读方法切换到真 `Task` | **推荐**：`Task` 的 final 不变量不动；壳只在 group Builder 内使用；占位期 listener/cancel 随 Guava `setFuture` 语义自然传播（含"先取消占位 → 连带取消真 future"） |
| (c) 声明期即建 token | token/上下文解析提前到 `builder()` 调用点 | 否决：deadline 预算是从 submit 起算的端到端预算（公理"deadline min(自己,父)"），提前建 token 会让声明—提交间隔吃掉预算，并改变"submit 在提交时解析上下文"的既有语义 |

(b) 的壳语义要点：

- 声明期（submit 前）读方法必须服从 `TaskFuture` 契约"任何生命周期点不阻塞、
  **不抛异常**"（`TaskFuture.java:38-45`）：`taskName()` → 声明名；
  `outcome()` → `RUNNING`（尚未终态，如实）；`deadlineNanos()` →
  `Long.MAX_VALUE`（无界，文档明示 submit 时才会获得真实 deadline）；
  `failure()` → `null`；`remaining()` → 由 MAX_VALUE 推导。**不能抛 ISE**——
  那会违反接口契约；"忘记 submit"的诊断靠 §5.2 的 Builder 关闭与 toString，
  不靠读方法抛异常。
- submit 后：壳的全部方法委托给 submit 现场创建的 `Task.of(name, token, future)`；
  占位 `SettableFuture.setFuture(task)` 同时完成值桥接与取消桥接。
- 声明期被 `cancel()` 的成员：submit 时照常走内核提交路径，`setFuture` 把取消
  传播给真 future，由既有成员直消级联统一归因（`MEMBER_CANCELED` → 组级联）。
  不为其特设跳过逻辑——避免第二条管道（判据 2）。注意这与缺陷 #3
  （`SlidingWindowSubmitter` bind 窗口竞态）同构，修复 #3 时一并核对。

### 4.2 combine 的形态：保留框架 join，换掉值访问机制

原提案"combine 改普通 Callable"若理解为**去掉框架调度、由用户自行
`future.get()` 汇合**，则被阻塞：

- 全量 join 当前由 `memberCompleted` 计数器唯一保证（`TaskGroup.java:275-302`）；
- 用户在普通任务体内 `get()` 会阻塞目标 Par 的 worker 等待兄弟成员，与有界池
  构成死锁面，且 `get()` 不观察 token/deadline，等待无界、脱离结构化取消——
  直接撞公理 1/3 与 `task-group-terminal-combine.md` §5/§6。

**推荐形态（综合方案）**：combine 仍由框架调度——只在全部成员成功后提交一次
（`submitTerminalOnce`，`TaskGroup.java:232-244`）——但函数签名从
`CombineFunction.apply(CompletedTaskValues)` 改为**普通 `Callable<R>`**，兄弟值
直接 `user.get()` 读取。因为框架保证 combine 提交时全员已 SUCCESS，这些
`get()` **永不阻塞**，死锁面不存在；`CompletedTaskValues`/`CombineFunction` 两个
类型随之删除，内核零改动。这正是"类型随返回值流动"在 combine 上的体现。

### 4.3 声明期校验（把运行期错误提前到配置期）

- `par.globalPar() == builder 的 global`：`Par.globalPar()` 已存在
  （`Par.java:49`），`task()/combine()` 声明时即可校验，错配立即 IAE——
  现在是 submit 解析时才暴露（`TaskGroup.java:485`）。
- 重名校验：声明点抛（现状：`TaskGroupDefinition.Builder.task` L166 已有，保留）。
- 连带简化：`GlobalPar.Builder.register(String, ExecutorService)` 返回 `Par`；
  `GlobalPar.par(String)` 接受裸字符串（现状只有 `ParName` 重载，
  `GlobalPar.java:140,415`）。

## 5. 焦点问题 4：生命周期问题逐条

### 5.1 占位 future 的 token 附加（原"问题 1"，阻塞性）——已由 §4.1(b) 解决

声明期确实没有 token：group token 在 `submit()` 现场创建
（`TaskGroup.java:472-473`），成员 token 在 `MultiTaskContext.resolve`
（`MultiTaskContext.java:131`）。路径 (b) 不要求声明期有 token：壳的读方法在
submit 前回答声明期值（§4.1），submit 后委托真 `Task`。`Task` 不变量改写为：
**壳的身份（name）建时固定；token/deadline 只在 submit 时存在且只赋一次；
submit 前后读方法答案不同是文档化的生命周期阶段差异**（类似既有"未 bind 的
member token 永远 RUNNING"先例，`TaskGroup.java:172-175`）。

附带守卫：`CancellationToken.bind` 的 `futureToken.setFuture`（L161）在
"bind 前 cancel"窗口不会传染取消。新形态下壳的 cancel 走占位 future 而非
token，不扩大该窗口；但缺陷 #4（bind 对已过期 deadline 不同步提交 TIMEOUT，
`CancellationToken.java:133-139`）在同一片代码，修复时一并核对。

### 5.2 忘记 `submit()`（原"问题 2"）

占位 future 永不完成 ⇒ `get()` 永久挂起。对策（按公理 2"让忘记变显眼"）：

- `Builder` 实现 `AutoCloseable`：`close()` 时若从未 submit，以
  `LeanCancellationException` 失败全部占位（`get()` 立即抛，不挂死）；
- 推荐用法写成双层 try：

  ```java
  try (TaskGroup.Builder tasks = TaskGroup.builder(global, "page", Duration.ofSeconds(3))) {
      TaskFuture<User> user = tasks.task("get-user", userPar, userService::getUser);
      try (TaskGroup group = tasks.submit()) {
          render(user.get());
      }
  }
  ```

- 占位 `toString()` 带诊断（组名、成员名、"declared at …, never submitted"）；
- 评估后否掉的更强兜底：读方法抛 ISE（违反 `TaskFuture.java:38-45` 契约）、
  GC/finalizer 检测（不可靠且引入隐式状态）。
- 已知残余风险：Builder 既不 submit 也不 close 时仍挂起——文档唯一推荐用法即
  双层 try，使"忘记"需要同时忘记两件事。

### 5.3 Builder 误用（原"问题 3"）

- `submit()` 单次使用：第二次调用抛 ISE（Builder 状态机：OPEN → SUBMITTED/CLOSED）。
- 声明后丢弃 Builder 变量：同 5.2，close 路径兜底。
- 成员 `Par` 不属于该 `GlobalPar`：§4.3 声明期校验。

### 5.4 `close()` 语义（原"问题 4"）

try-with-resources 模式在新 API 下**仍然成立**且更清晰：`submit()` 返回的
`TaskGroup` 语义不变（`close()` = 未完成则等同 `cancel()`，
`task-group-api-and-options.md:153-154`、
`task-group-cancellation.md:148-153`）；新增的只是 Builder 侧 5.2 的关闭语义。
"submit 前 close"发生在 Builder 上而非 group 上，两层职责不混淆。

### 5.5 成员 future 的归因漂移（原"问题 5"）——**不是缺陷，维持现状并文档化**

机制：成员被直接取消时，`ScopedCallable` 现场记 `MEMBER_CANCELED`；
`TaskGroup` 随后 `groupToken.cancel()`（`TaskGroup.java:280-289`）沿传播链把
成员 token 改写为 `PROPAGATED_CANCELED`，之后该 future 的 `outcome()` 读 token
链得 `GROUP_CANCELED`。

前序会话已确认的结论（本提案沿用）：**组级归因以收敛快照为准，future 级读
token 链，两者对同一事件的答案可以不同**。该分叉已双重固化：
`TaskFutureTest` 测试 + `TaskFuture.java:41-45` javadoc（"terminal value can
still be refined…"，并明确举了 MEMBER_CANCELED→GROUP_CANCELED 的例子）。
契约依据：`task-group-cancellation.md` §8.4.1（L126-129 有意分叉）。
本提案不做改动，只在 user-guide 把这段 javadoc 的说明同步成文。

## 6. 与缺陷 #5 的关系：一次改，不分两次

缺陷 #5「失败成员的组 outcome 随完成顺序漂移」（报告 L286-333）：窄改
`deriveOutcome()`（`TaskGroup.java:362-380`）——`SUCCESS`/`RUNNING` 分支在
`allSuccess` 判定前先查 `failedTaskName` 已记录的失败者。这与本提案动 group
归因是**同一片代码，必须一次改**。改动面（报告附录 A 已核对）：

- 实现：`TaskGroup.java:362-376` + `:355-361` javadoc + `:289-295` 注释；
- 测试：`ScopedTaskContractTest.java:96` 期望改 `USER_FAILURE`（重写 :93-95
  注释）、`TaskGroupTest.java:392-398` 收窄、新增跨顺序一致性回归用例；
- 契约：`task-group-lifecycle.md:240-241`、`task-group-cancellation.md:102-103,
  131-132`、`task-group-terminal-combine.md:161`、
  `task-group-api-and-options.md:276-280`、
  `task-group-observability-and-verification.md:112`（必测矩阵 #15 的 `FAILED`
  残留一并清理）、`CHANGELOG.md:25`。

风险边界：备选"成员失败即同步 failFastCancel()"会改变 mixed 场景 first-wins
竞态，**不采用**。缺陷 #1-#4 不属于本提案；#4 见 §5.1 的连带核对提示。

## 7. 目标 API 形态汇总

新增/变更（公开面）：

- `TaskGroup.builder(GlobalPar, String, Duration)` /
  `TaskGroup.inheriting(GlobalPar, String)`（嵌套：继承父 deadline 上限）；
- `TaskGroup.Builder`（嵌套公开类型）：`task(String, Par, Callable<T>)`、
  `task(..., TaskOptions)`、`combine(String, Par, Callable<R>)`（至多一个）、
  `submit()`（单次）、`close()`（5.2）；
- `TaskGroup` 保留：`completionFuture()`、`members()`、`findMember(String)`、
  `cancel()`、`close()`；`future(TaskKey)` 删除（无 key 可查）；
- `GlobalPar.par(String)` 重载；`Builder.register(String, ExecutorService)`
  返回 `Par`；
- 组完成通知：`completionFuture().addListener(...)`（Guava 语义天然满足原
  listener 契约：一次、结果固定后触发、异常隔离——Guava 捕获 listener 异常）。

## 8. 删除清单与波及面

**删除 7 个顶层类型 + 1 个嵌套类型**（原提案"−6"漏算 `TaskGroupOptions`，
净删数按 7 计）：

| 类型 | 删除理由 | 引用面（src 词级计数，含测试） |
|---|---|---|
| `TaskGroupDefinition`（+`Builder`/`TaskDefinition`/`CombineDefinition` 3 嵌套） | 被 `TaskGroup.Builder` 取代 | 133 / 9 文件 |
| `TaskKey` | 类型随返回值流动 | 168 / 12 文件 |
| `ParName` | 裸字符串 + `Par` 句柄取代 | **325 / 15 文件（波及最大）** |
| `CombineFunction` | combine 改普通 `Callable` | 11 / 5 文件 |
| `CompletedTaskValues` | 兄弟值直接 `future.get()` | 8 / 4 文件 |
| `TaskGroupListener`（+嵌套 `TaskGroupEvent`） | `completionFuture().addListener` | 15 / 5 文件 |
| `TaskGroupOptions` | 折叠进两个工厂参数 | 50 / 10 文件 |

`PublicApiSurfaceTest` 白名单：顶层 −7，嵌套 −4（`TaskGroupDefinition$Builder/
$TaskDefinition/$CombineDefinition`、`TaskGroupListener$TaskGroupEvent`），
新增 `TaskGroup$Builder`。

**`ParName` 的特殊性**：它不止服务 group——`GlobalPar` 装配路径全部以它为 map
键（`GlobalPar.java:47-52,140,391,415,422`），batch 路径经 `Par` 持有
（`Par.java:65,116,139`）。移出公开 API 意味着 `register/par/defaultPar/
parTaskListener` 全部改签名。建议**与 group 重做同一方向、独立提交**，避免一个
commit 同时翻 group 与装配面。

**契约/文档同步清单**：`design/task-group-*.md` 六份（不止 user-guide）；
`idea-graveyard.md` L203-227（invokeAll 否决的类型安全理由改写）、
`philosophy.md` 刻意局限清单（L429-433，"不支持异构任务组合"需注明已被
TaskGroup 演进取代）；新建 `docs/{en,zh}/migration-v0.3.md`；user-guide group
章节重写；`CHANGELOG`；ADR 无需新增或 Supersede（无 group API 相关 ADR，
决策载体在 design/ 契约系列）。

## 9. 待拍板决策（附推荐）

1. **净删 7 个（含 `TaskGroupOptions`）** —— 推荐。删 listener 后 options 只剩
   name+timeout，没有独立存在的理由。
2. **`ParName` 一并移出公开 API** —— 推荐一并做、独立提交（先 group 后装配面）。
   终态最优是裸字符串；若本轮想收窄爆炸半径，可留到 v0.3 内第二个 commit。
3. **声明期占位读语义** —— 已被 `TaskFuture` 契约收窄为唯一解：`outcome()` →
   `RUNNING`、`deadlineNanos()` → `Long.MAX_VALUE`、`failure()` → `null`，
   不抛异常（§4.1）。此项不再是开放问题，列出仅为确认。
4. **缺陷 #5 与本提案一次改**（同区代码）—— 推荐。实现/测试/契约文档按 §6
   清单一次同步。
5. **Builder 生命周期形态** —— 推荐 `AutoCloseable` + 双层 try 为唯一文档用法
   （§5.2）。备选：Builder 不 closeable、纯文档+诊断（少一层嵌套，但"忘记
   submit"重新变成挂死）。
6. **combine 形态** —— 推荐 §4.2 综合方案（框架保留 join 调度 + 普通
   `Callable` + `future.get()` 读兄弟值），而非"用户自行 join"。
