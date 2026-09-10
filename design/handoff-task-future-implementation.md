# HANDOFF：实施 Task/TaskFuture（增强 ListenableFuture）→ group API v0.3

> 交接对象：零上下文的实现会话。本文自包含：目标、已定决策及**理由**、代码锚点、
> 实施步骤、验收标准、坑位清单。设计全文在引用文档里，本文是执行计划。
> 仓库：`/Users/linqh/IdeaProjects/vformation`（Maven，`io.github.monadrome:parallel-in-scope:0.2.0`，
> Java 8 源码 / 测试 release 11 / Guava 33.6.0-jre / TTL 2.14.5）。

## 0. 先读（按序）

1. `AGENTS.md`（仓库根）——构建命令、代码风格、Git 工作流。关键规矩：
   - `mvn test` / `mvn test -Dtest=<Class>#<method>` / `mvn spotless:apply` / `mvn clean verify`；
   - `src/main/java` 仅 Java 8 API；访问器用 `x()` 裸风格，禁用 `getX()/isX()`；
   - 每包 `package-info.java` + `@ParametersAreNonnullByDefault`；public API 用
     `javax.annotation.Nullable`，内部用 checkerframework 的（均 provided 作用域）；
   - 日志走 JUL；默认不写注释；
   - 验证通过后按 conventional commit（`feat:` 小写摘要）提交并推送当前分支；
     只 stage 属于本变更的文件。
2. `design/task-future-design.md` —— **本次实施的契约**（目标/约束/不变量/API/实现方式/
   测试矩阵）。若本文与它冲突，以它为准并回报冲突。
3. `design/group-api-redesign-v0.3-proposal.md` —— Phase 2 的契约。
4. `design/enhanced-listenablefuture-analysis.md` —— 内核事实调查报告（含全部行号证据）。

## 1. 任务总览

- **Phase 1（本交接主体）**：新增 public 接口 `TaskFuture<T> extends ListenableFuture<T>`
  与 public final 类 `Task<T> extends ForwardingListenableFuture<T> implements TaskFuture<T>`，
  把内核已有的任务名/终态归因/deadline 数据以只读视图下放到用户手里的 future；
  改造所有交付点使"库交付的每个任务执行 future 都 instanceof TaskFuture"。
- **Phase 2（Phase 1 落地并验证后）**：按 v0.3 提案重构 group 用户表面
  （`TaskGroup.Builder`、声明期返回 `TaskFuture<T>` 占位、删 6 个公开类型）。
  Phase 2 依赖 Phase 1 的占位壳，不要并行做。

## 2. 已定决策（含理由——不要重新翻案）

| # | 决策 | 理由（重新讨论前先读这条） |
|---|---|---|
| D1 | `Task` 继承 `ForwardingListenableFuture`，**不是** `FluentFuture` | Guava 33.6.0 `FluentFuture()` 构造器包私有，包外继承不可能；官方 javadoc Extension 节推荐的正是"ListenableFuture 子接口 + from() 适配 + ForwardingListenableFuture 实现"（design 文档 §2 有原文引用） |
| D2 | delegate **final，创建时固定，永不交换**（不变量 I2） | 桥接前用户可能已 addListener/cancel 占位，这些登记落在占位本体上；换 delegate 会丢。桥接仍走 `placeholder.setFuture(real)`：占位先取消则 setFuture 返回 false 并级联取消真实 future——这是现有语义，必须原样保留 |
| D3 | `Task` 不进执行管道（不变量 I5） | executor 只认 `ExecutionPhaseHintFuture`（purge 观察、精确 runner 中断、inline fallback 都依赖它）；`Task` 只是用户侧视图。内核需要引擎能力时持裸引用 |
| D4 | 接口只放 5 个方法：`taskName()/outcome()/deadlineNanos()/remaining()/failure()` | 每个方法都过了"消除哪类误用"检验；时间戳/索引/链式/cancel(reason)/executorName 已评估并**拒绝**（取舍表见 design 文档 §6，含逐项理由）。不要顺手加方法 |
| D5 | `outcome()` 归因语义（2026-09 修订，解决实测冲突） | 未终态→RUNNING；终态后**任意时刻读同一值**。活读 token 不够：组会因成员取消而随后取消并改写 member token（实测 MEMBER_CANCELED→GROUP_CANCELED 漂移）；冻结 listener 有注册顺序脆弱性。采用**有序归因规则**：① directCancel 标志（`Task.cancel()` override 置旗，I6 保证级联不经过壳 ⇒ 标志=用户直接取消=发起者）→ ② kernelOutcome（abandon 等内核路径写入）→ ③ 自身 token TIMEOUT → ④ scopeToken（组=组 token，批=批 token，与 `TaskGroup.classifyCancelled` 同一规则抽共享 helper，机制零改动）。①必须排在③前：member token 的 deadline timer 在用户取消后仍会触发改写 token。视角约定：future 与成员快照同为成员视角且一致；`TaskGroupResult` 是组视角，可不同。按 (b) 写的"不稳定语义"文档与测试需按此改写 |
| D6 | `submitCanceller` 保持裸 `ListenableFuture` | 它是控制句柄不代表任务执行（约束 C6）；completionFuture 要包装（名字=组名，token=组 token） |
| D7 | 占位 abandon 的失败路径包 `SubmissionException` | 否则 `SUBMISSION_FAILURE`/`USER_FAILURE` 区分在占位上失效（`ExecutionPhaseHintFuture.outcome()` L212-219 的区分靠它） |
| D8 | 不加 fluent 链方法 | 需要时用户 `FluentFuture.from(task)`；派生 future 是普通 FluentFuture，增强不流入用户编排链（与 idea-graveyard 编排否决一致） |
| D9 | equals/hashCode 保持对象身份；`toString` 重写为诊断串 | Guava `ForwardingObject` 不转发这三个方法；future 实践中按身份比较 |
| D10 | `remaining()` 负值钳到 `Duration.ZERO` | 过期后归一，避免负时长泄漏到用户代码 |
| D11 | I6 取消级联绕过壳 | `CancellationToken.bind` 输入、组 cancel、fail-fast 级联一律用 `kernelDelegate()` 裸引用，绝不调 `Task.cancel()`——否则级联会误置 directCancel，连带者被错归 MEMBER_CANCELED |
| D12 | 不做冻结 listener | §5.2 规则的四个输入在 future 终态时全部定型（级联先 CAS 后取消、置旗先于转发、kernelOutcome 先于占位完成），终态后任意时刻计算结果相同；冻结 listener 有注册顺序脆弱性，不用 |

## 3. 代码锚点（行号来自调查时点，编辑前用 Grep/Read 复核）

| 位置 | 现状 | 改造 |
|---|---|---|
| `src/main/java/io/github/monadrome/parallelinscope/` 新增 `TaskFuture.java`、`Task.java` | — | public；`Task` 构造包私有，工厂 `of(name, token, delegate)` / `placeholder(name, token)`，包私有 `bind(real)` / `abandon(...)` |
| `TaskSubmissions.java:49-55` `prepare(...)` | 返回 `ExecutionPhaseHintFuture` | 出参处包装为 `Task.of(...)`；`ExecutionPhaseHintFuture` 目前**不持有** context/token——prepare 需多传一根 token 引用（数据在 `MultiTaskContext.cancellationToken()` 现成） |
| `SlidingWindowSubmitter.java:88-90` | 裸 `SettableFuture` 占位 | 改 `Task.placeholder(name, token)`；结果列表元素类型改 `Task<V>` |
| `SlidingWindowSubmitter.java:161, 187` | 两处 `(SettableFuture<V>) result.get(index)` 向下强转 | 改 `task.bind(real)` / `task.abandon(...)` 自述方法 |
| `SlidingWindowSubmitter.java:187-191` abandon | `setException(reason)` / `cancel(true)` 无归因 | 失败路径包 `SubmissionException`（D7）；`cancel` 路径归因由 token 读出，不用改 |
| `SlidingWindowSubmitter.java:73-76` | `Futures.immediateFailedFuture` 填充 | 包装为 `Task.of(name, token, immediateFailedFuture(SubmissionException 包裹))` |
| `TaskGroup.java:60` | `SettableFuture<TaskGroupResult>` | 改 `Task`（组名、组 token） |
| `TaskGroup.java:495-496, 536-537` | `Par.prepareGroupTask` 产成员/combine future | 包装为 `Task`（成员名=TaskKey 名，成员 token） |
| `TaskGroup` 成员访问器 `future(key)/members()/findMember()` | 返回 `ListenableFuture` | 声明收窄为 `TaskFuture` |
| `TaskBatchResult.results()` | `List<ListenableFuture<T>>` | `List<TaskFuture<T>>`；`outcomeOf`（L163-176）走接口 |
| `FutureInspector.java:28-45` | `instanceof ExecutionPhaseHintFuture` 反推 outcome | 改为对 `TaskFuture` 接口编程；保留裸 future 保守 fallback（submitCanceller 等路径仍经过） |
| `TokenOutcomes.java:41-58` | token 状态→TaskOutcome 归因表 | `Task.outcome()` 复用，不复制逻辑 |
| `src/test/.../PublicApiSurfaceTest.java:20-48` | 28 个 public 顶层类型白名单 | +`TaskFuture` +`Task` = 30 |

归因实现：`Task.outcome()` = `FutureInspector` 逻辑上移——done 非 cancelled：
成功→SUCCESS，失败按 `SubmissionException` 区分；cancelled→
`TokenOutcomes.forCanceled(token.state()/originState())`；未完成→RUNNING。
`failure()` 取 `FutureInspector.exceptionNow` 的 cause。

## 4. 实施步骤（按序，每步可验证）

1. 建 `TaskFuture.java`（接口 + javadoc 写明归因语义/线程安全/`instanceof` 用法）
   与 `Task.java`（结构见 design 文档 §5.1）。`mvn compile`。
2. 改 `TaskSubmissions.prepare` 包装 + token 引线。`mvn compile`。
3. 改 `SlidingWindowSubmitter`（占位、两处强转、abandon、初始失败）。
   跑批路径既有测试：`mvn test -Dtest='*Batch*'`（先确认现状绿再改）。
4. 改 `TaskGroup`（成员/combine/completionFuture 包装、访问器收窄）。
   `mvn test -Dtest='*TaskGroup*'`。
5. 改 `TaskBatchResult` + `FutureInspector`。`mvn test`。
6. `PublicApiSurfaceTest` 白名单 +2。
7. 新增 `TaskFutureTest`（或按现有测试命名习惯）覆盖 §5 测试矩阵。
8. `mvn spotless:apply && mvn clean verify` 全绿。
9. 文档：`docs/en/user-guide.md` + `docs/zh/user-guide.md` 增补 TaskFuture 章节、
   `docs/en/migration-v0.3.md`（如无则建）记返回类型收窄。AGENTS.md 要求 API 变更与
   实现/测试/文档/迁移笔记一次变更提交。
10. 提交推送（conventional commit，`feat:` 起）。**注意**：`design/` 下三份提案文档
    目前未提交、属设计讨论产物——本变更只 stage 实现+测试+用户文档；design 文档
    是否随附提交由用户定夺，不主动带入。

## 5. 测试矩阵（逐条对应 design 文档 §8）

1. 交付完整性：批（窗口内/窗口外/初始失败）、组成员、combine、completionFuture
   全部 `instanceof TaskFuture`；`submitCanceller` 不是。
2. `outcome()` 全枚举：SUCCESS/USER_FAILURE/SUBMISSION_FAILURE/MEMBER_CANCELED/
   GROUP_CANCELED/FAIL_FAST/TIMEOUT/RUNNING；嵌套场景走 `originState()` 链。
3. 竞态：cancel vs failure；checkpoint 异常赢级联（`causedByCancellation`）；
   deadline 与成功同时；终态后多次读一致（I3）。
4. 占位桥接：桥接前 addListener 的 listener 桥接后触发；桥接前 cancel →
   真实 future 被级联取消；abandon 两路径归因（D7）。
5. 委托透明：get 结果/异常、cancel(true) 中断 worker、addListener(executor) 语义。
6. `remaining()` 单调不增、过期钳 ZERO；`deadlineNanos()` 与 token 一致。
7. completionFuture：组名、组 token 归因、成功时 `outcome()==SUCCESS`。

## 6. 坑位清单（每个都对应一个真实机制）

- **不要交换 delegate**（D2）：listener 登记在占位本体上，换了就静默丢回调。
- **不要动 `setFuture` 的取消级联**：占位先 cancel 时 `setFuture` 自动级联取消
  真实 future（Guava 语义），`bind()` 只是薄封装，别重新实现取消逻辑。
- **`ExecutionPhaseHintFuture` 不是 `Task` 的 delegate 的替代**：两者并存——
  前者是引擎对象（进 executor），后者是用户视图（不进 executor）。
- **groupId 不上 future**：组对象晚于成员 future 创建（`TaskGroup.java:471` vs L554），
  需要回填，已判定不做。
- **Java 8**：无 `var`/`List.of`/`Map.of`；`ForwardingListenableFuture.delegate()`
  签名是 `ListenableFuture<? extends T>`。
- **0.x 允许二进制破坏**，但返回类型收窄要写进迁移笔记（源码兼容、二进制不兼容）。
- 行号会漂移：以上行号是调查时点快照，一律先定位再改。

## 7. 验收标准

- `mvn clean verify` 全绿（含 `PublicApiSurfaceTest` 更新后）。
- §5 测试矩阵 7 组全有用例且通过。
- 用户文档与迁移笔记同步；`AGENTS.md`/`design/AGENTS.md` 路由信息若因实现
  变动而过时则一并更新。
- 交付声明收窄后，批/组既有行为的语义零变化（取消、deadline、TTL、fail-fast、
  purge 全部原样）——这是"只加视图、不动机制"的试金石。
