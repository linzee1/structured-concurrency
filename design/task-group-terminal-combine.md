# TaskGroup 终端汇合设计

> 状态：提案。本文定义 `TaskGroup` 的可选终端汇合能力，不以既有实现作为约束。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。

## 1. 目标

请求常先并行读取数个独立资源，再组装最终值：

```text
load-user ──┐
load-orders ├── assemble-page ──> AccountPage
load-stock ─┘
```

调用方自行等待 member future 再组装（如 `Futures.whenAllSucceed(...).call(...)`），会泄漏等待、失败处理、执行器选择和取消边界。本设计在 Group 内表达这个 fan-out / join / final-call 形状，同时禁止它演变成任意 DAG。

## 2. 范围

一个 Group 有零至多个相互独立的 **member**，以及零或一个 **terminal combine**。

- member 彼此没有依赖；
- combine 依赖所有 member，且只在每个 member 成功后才提交执行；
- 无 combine 时，Group 保持"成员全部终态即完成"的语义，不新增任何 API 表面；
- 有 combine 时，Group 还要等待 terminal future 终态。

## 3. 核心模型：带 join 前提的 member

combine 不是 completion listener，不是新调度原语，而是一个**全部运行期准备都在 submit 完成、仅 executor 提交被推迟到 join 条件满足时**的特殊任务：

- submit 准备阶段（与 member 同步、在提交线程上、在同一个 `GlobalPar.whileOpen()` 内）完成：combine token（group token 的 child）、`TaskExecutionContext`、TTL 快照、结构 parent、observation、terminal future 的创建与注册；
- join 时（全部 member 成功收敛之后）只做一件事：对已 prepared 的 terminal future 调用目标 executor 的 `execute()`（包 `SubmissionScope`、在 Group lock 外）；
- 除提交时机外，combine 复用 member 的全部机制：取消级联、deadline 计算与升级、rejection 处理、`TokenOutcomes` 归因、TaskListener 事件、`retainUntilComplete()`。

直接推论：

- combine 的用户 lambda 只在 join 后、在目标 executor 线程上**执行**；准备阶段创建的是执行管道，不触碰用户 lambda。它不得在 builder、`build()`、submit 准备阶段或 member 完成回调中运行；
- TTL 快照时点与 member 一致（submit 的 prepare 阶段），不捕获 join 回调线程的上下文；
- combine 在 submit 时已完成 GlobalPar admission 与 retain；join 时的提交不再做 `whileOpen()` 检查，因此"submit 后 `GlobalPar.close()` 与 join 竞争"不产生新问题——组被完整接纳后 combine 照常提交并终态；
- combine 禁用 CPU-bound 拒绝后的 inline fallback：join 时的提交线程是收敛回调线程，不存在可借用的调用方线程，inline 的语义基础不成立。被目标 executor 拒绝一律记 `SUBMISSION_FAILURE`；
- combine 的结构 parent 与 member 相同（submit 现场的外层 scoped task 或 null），MUST NOT 把最后完成的 member 当作结构 parent。

完成计数不变式调整为 `terminalCount == memberCount + (combine ? 1 : 0)`：member 非成功导致 combine 不执行时，框架必须把 terminal future 推向终态（按 token 归因取消），不得遗留 pending public future。

## 4. 建议 API

combine 注册在 definition 上，与 member 一样经 `TaskGroup.submit(global, definition)` 冻结并统一提交；definition 保持不可变、可重复提交。`TaskRef<T>` 继续是名称和类型的单一事实来源，combine 同样持有自己的 ref：

```java
TaskRef<User> user = new TaskRef<User>("user") {};
TaskRef<List<Order>> orders = new TaskRef<List<Order>>("orders") {};
TaskRef<Inventory> inventory = new TaskRef<Inventory>("inventory") {};
TaskRef<AccountPage> page = new TaskRef<AccountPage>("assemble-page") {};

TaskGroupDefinition definition = TaskGroupDefinition.builder(groupOptions)
        .task(user, "database", () -> users.load(request.userId()), userOptions)
        .task(orders, "http", () -> orderClient.load(request.userId()), orderOptions)
        .task(inventory, "inventory", () -> inventoryClient.load(), inventoryOptions)
        .combine(
                page,
                "cpu",
                values -> new AccountPage(
                        values.value(user),
                        values.value(orders),
                        values.value(inventory)),
                combineOptions)
        .build();

try (TaskGroup group = TaskGroup.submit(global, definition)) {
    ListenableFuture<AccountPage> pageFuture = group.future(page);
    TaskGroupResult result = group.completionFuture().get();
}
```

汇合函数收到的视图只提供已成功的值，不暴露 future，也不执行等待：

```java
@FunctionalInterface
public interface CombineFunction<R> {
    R apply(CompletedTaskValues values) throws Exception;
}

public interface CompletedTaskValues {
    <T> T value(TaskRef<T> ref);
}
```

要点：

- `combine(ref, executorName, function, options)` 在配置期校验：参数为 null、ref 名称与任一 member 或 combine 重复，立即拒绝；一个 definition 至多一个 combine。executor 只接受注册名（submit 时经 `GlobalPar.par(executorName)` 解析，未知名称抛 `IllegalArgumentException`），不提供 `Par` 实例重载，与 member 对称；
- `TaskGroup` 不新增泛型参数，也不新增 `resultFuture()`：terminal future 通过既有的 `group.future(combineRef)` 取回，类型安全、未知 ref 校验和 Guava 终态语义全部复用成员路径；
- `combineOptions` 即 `MultiTaskOptions`，读取 name/timeout/taskType/rejectEnqueue，与 member 一致；不能覆盖 Group 的取消策略，listeners 仍只有组级读取；
- 没有 combine 的 Group 不创建任何虚假终端任务，也不存在 `Void` 特殊路径。

## 5. CompletedTaskValues 契约

- `value(ref)` 不阻塞；combine 运行时所有 member 已成功；
- ref 必须属于该 Group 的 member，且其 raw 结果类型必须覆盖注册类型，否则抛 `IllegalArgumentException`（与 `group.future(ref)` 同一套校验）；combine 自己的 ref 不可读；
- 成功 member 的返回值可以为 null；
- 成员结果本就保存在成员 future 中；视图只在 `apply` 调用期间有效，实现可以在回调返回后释放结果引用；
- 不提供 `Map<String, Object>`，避免强转和名称重构风险；
- 不提供 future，避免 combine 重新等待、取消或编排底层任务。

combine 不是用户编写的 future 编排器，而是框架确认 join 条件后的单次业务计算。

## 6. 生命周期与提交时序

`submit` 分为三步：

1. 在一次 `GlobalPar.whileOpen()` 内冻结 member registry 和 combine definition，创建 group token、全部 member futures、terminal future 及 combine 的全部运行期管道（token/context/TTL 快照/结构 parent），全量注册并发布 registry，安排 group deadline timer；
2. 提交 members，按结构化规则处理失败、拒绝、超时和取消；
3. 所有 members 成功后构造 `CompletedTaskValues`，对已 prepared 的 terminal future 调用目标 executor 的 `execute()`；combine 终态后 Group 才完成。

空 Group 配置 combine 时，join 条件立即满足，combine 仍提交到显式 executor；此时"空组不创建 timer"的既有规则不再适用——group deadline timer 必须为 combine arm 上。空 Group 无 combine 时立即成功，不变。

## 7. 结果与 outcome

terminal future 保持普通 Guava 语义，与 member future 一致：成功返回 `R`、失败抛 `ExecutionException`、取消表现为 cancelled。`completionFuture()` 继续以正常 future 完成并返回 `TaskGroupResult`，Group outcome 是结果数据，不用异常编码。

| 情况 | combine | terminal future | Group outcome |
|---|---|---|---|
| 全部成功 | 执行并成功 | 成功返回 `R` | `SUCCESS` |
| member 非成功 | 不执行 | 按 group token 归因取消（`FAIL_FAST`/`TIMEOUT`/`GROUP_CANCELED`） | member 的组级 outcome |
| combine 用户失败 | 执行 | 失败（`ExecutionException`） | `USER_FAILURE` |
| combine 被拒绝 | 提交但被拒（无 inline） | 失败 | `SUBMISSION_FAILURE` |
| combine 自身 deadline 先到 | 升级 `groupToken.timeoutCancel()` | 取消 | `TIMEOUT` |
| group deadline 先到 | 不执行或中断 | 取消 | `TIMEOUT` |
| `group.cancel()` / close | 不执行或中断 | 取消 | `GROUP_CANCELED` |

`TaskGroupResult` 增加 `@Nullable TaskCompletion<?> terminal()`（无 combine 或未执行时为 null，遵循执行前取消 member 不伪造事件的惯例）。`members()`/`memberCount()` 保持只含 member。combine 失败或拒绝时 `failedMemberName()` 取 combine 的注册名：combine failure 不能伪装成 member failure，但对调用方呈现在同一个字段中。

## 8. 取消、deadline 与归因

- combine token 是 group token 的 child，与 member 同层；group 取消经 token 构造期的 parent 监听级联到 combine，未开始的 combine 不得执行，运行中的 combine 接收协作取消和中断请求；
- member 失败时 combine 不执行，terminal future 由框架终结，归因与 sibling member 相同；
- combine deadline 是 `min(combine requested deadline, group deadline)`；`inheritTimeout()` 解析为 group deadline；与 member 相同的 bind 跳过策略适用（解析出与组相同的 deadlineNanos 时跳过单独 bind）；
- combine 自身 deadline 先到时，与 member 规则一致：token 上的 timeout 监听器调用 `groupToken.timeoutCancel()`，Group 固定 `TIMEOUT`；
- group deadline 从 submit 起计算，涵盖 fan-out 等待和 combine 运行，combine 不重新获得一整段 group timeout；
- 归因走 `internal/TokenOutcomes` 同一张映射表，不新增映射；combine 的失败、直消或超时将 Group 固定为相应 outcome，members 已终态，无 sibling 可回撤；
- combine 复用 Group 的 token 体系、`GlobalPar.timeoutScheduler()` 和 `TaskSubmissions` 两阶段内核，不创建新 executor 或 timer service。

## 9. 观测与 TaskGraph

- combine 正常产生 `TaskCompletion` 和 TaskListener 事件；执行前取消不伪造事件，与 member 一致；Group listener 仍只在完整结果发布后调用一次；
- membership 仍不产生 member-to-member 图边。但 combine 是真实的 all-to-one 依赖：

```text
member A ──┐
member B ──┼──> combine
member C ──┘
```

- v1 采用 **telemetry-only**：members→combine 的 join 关系只写入 Group telemetry，不写 TaskGraph 边，不参与 deadlock 环检测；MUST NOT 伪造单父边或虚构 group batch 节点。图模型的 multi-parent 扩展是独立工作，不阻塞 combine 交付，完成后再把真实边写入图。

## 10. 与 idea-graveyard 的关系

`docs/zh/design/idea-graveyard.md` 拒绝的是通用链式编排与任意 DAG：用户自定拓扑、异常恢复策略、结果变换链。本设计不与之冲突，边界在于——依赖形状固定为单个全量 join，无用户拓扑、无 fallback、无中间阶段；combine 的价值在于结构化取消、deadline 与观测，而不是编排表达力。采纳本文时应同步修订 graveyard 条目，把"单一终端全量 join"列为有理由的例外并指向本文，避免未来维护者无法区分有意演进与意外漂移。

## 11. 非目标

本设计不提供多个 combine、部分依赖、combine 后派生任务、`dependsOn(...)`、拓扑排序、任意 DAG，或将 member failure 转成 fallback 值。它们需要完整处理 ready-set 调度、循环、部分成功、依赖失败、取消传播和图观测，不能伪装成一个 Group 便利方法。

## 12. 优点与缺点

优点：它直接表达并行获取后的组装；调用方不再编写多 future 等待和执行器切换；终端计算可取消、可观测、受 deadline 约束；`TaskRef<T>` 保留类型安全且无需改动 `TaskGroup` 的泛型形态；combine 复用 member 机制，新增表面集中在"提交时机"一点；单一全量 join 控制了 API 的扩张。

缺点：

1. **概念增多。** 用户要区分 member、listener 和 combine；仅做观测或分别消费结果时，combine 没有价值。
2. **Java 8 泛型不够自然。** `values.value(ref)` 比二元或三元函数冗长，但比 `Map<String, Object>` 更安全；语言本身无法自动从异构 refs 推导 lambda 参数列表。
3. **失败面扩大。** 组装成为可失败、拒绝、取消、超时的任务，结果、指标、测试和文档都要扩展。
4. **额外一次调度。** 显式 executor 避免污染最后完成 member 的线程（禁用 inline fallback 后这一点是保证而非选择），但对纯字段拼装带来排队和上下文切换开销。
5. **端到端预算更紧。** member 用掉大部分 deadline 后，combine 可能尚未开始即超时；这是正确的端到端语义，但需清晰告知用户。
6. **图表达滞后。** v1 的 join 关系只在 telemetry 中可见，图诊断暂时看不到这条真实依赖。
7. **会引出 DAG 需求。** 用户可能接着要求部分依赖或多个阶段。必须坚守一个全量、末端 combine 的限制。

## 13. 采用门槛与验收

combine 仅用于需要框架调度与观测的非平凡业务计算。只记录指标时用 Group listener；调用方需要逐项消费结果时读 member futures；需要部分结果、fallback 或多依赖节点时另行设计 workflow/DAG API。

最低验收：

1. 所有 members 成功时 combine 恰好执行一次，callable 在指定 executor 线程运行，即使被拒绝也不 inline 到收敛回调线程（拒绝记 `SUBMISSION_FAILURE`）；
2. 任一 member 非成功时 combine callable 不执行，terminal future 按 token 归因终态，不留下 pending public future；
3. combine 的 token、`TaskExecutionContext`、TTL 快照和结构 parent 都在 submit 准备阶段创建：TTL 捕获时点与 member 一致，结构 parent 是提交现场的外层任务而非最后完成的 member；
4. combine 在 submit 时完成 admission/retain：submit 返回后 `GlobalPar.close()` 与 join 竞争时，combine 仍正常提交并终态；
5. combine 能通过 `TaskRef` 无阻塞取得正确值；unknown ref、raw 类型不覆盖、ref 名称冲突和 null 成功结果符合契约；
6. combine 自身 deadline 先到时升级为组 `TIMEOUT`；group deadline 涵盖 fan-out 与 combine；
7. combine 的失败、拒绝、直消与 close 有确定 outcome，`failedMemberName()` 取 combine 注册名，combine failure 不伪装成 member failure；
8. `terminalCount` 目标含 terminal future；completion future 等 terminal future 终态后才完成；Group listener 只调用一次；
9. TaskGraph 不写 membership 边、不伪造 join 边；join 关系出现在 Group telemetry；
10. 所有执行、拒绝和取消路径恢复 ThreadLocal/TTL。
