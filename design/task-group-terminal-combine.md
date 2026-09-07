# TaskGroup 终端汇合设计

> 状态：提案。本文定义 `TaskGroup` 的可选终端汇合能力，不以既有实现作为约束。

## 1. 目标

请求常先并行读取数个独立资源，再组装最终值：

```text
load-user ──┐
load-orders ├── assemble-page ──> AccountPage
load-stock ─┘
```

调用方自行等待 member future 再组装，会泄漏等待、失败处理、执行器选择和取消边界。本设计在 Group 内表达这个 fan-out / join / final-call 形状，同时禁止它演变成任意 DAG。

## 2. 范围

一个 Group 有零至多个相互独立的 **member**，以及零或一个 **terminal combine**。

- member 彼此没有依赖；
- combine 依赖所有 member，且只在每个 member 成功后执行；
- 无 combine 时，Group 保持“成员全部终态即完成”的语义；
- 有 combine 时，Group 还要等待 combine future 终态。

combine 是真实任务，不是 completion listener，也不是普通 member。

## 3. 建议 API

`TaskRef<T>` 继续是 member 名称和类型的单一事实来源。汇合函数收到的视图只提供已成功的值，不暴露 future，也不执行等待：

```java
@FunctionalInterface
public interface CombineFunction<R> {
    R apply(CompletedTaskValues values) throws Exception;
}

public interface CompletedTaskValues {
    <T> T value(TaskRef<T> ref);
}
```

调用侧只写一层业务 lambda：

```java
TaskRef<User> user = new TaskRef<User>("user") {};
TaskRef<List<Order>> orders = new TaskRef<List<Order>>("orders") {};
TaskRef<Inventory> inventory = new TaskRef<Inventory>("inventory") {};

TaskGroup<AccountPage> group = TaskGroup.builder(groupOptions)
        .task(user, "database", () -> users.load(request.userId()), userOptions)
        .task(orders, "http", () -> orderClient.load(request.userId()), orderOptions)
        .task(inventory, "inventory", () -> inventoryClient.load(), inventoryOptions)
        .combine(
                "assemble-page",
                "cpu",
                values -> new AccountPage(
                        values.value(user),
                        values.value(orders),
                        values.value(inventory)),
                combineOptions)
        .submit(global);

ListenableFuture<AccountPage> page = group.resultFuture();
```

第二个 `combine` 参数是已注册 `Par` 的名称。也可选用 `Par` 实例，但不能两种形式同时提供；推荐名称，以便 definition/plan 不保留运行环境对象。`combineOptions` 描述终端任务的名称、deadline、任务类型和 rejection policy，不能覆盖 Group 的取消策略。

没有 combine 的 `TaskGroup<Void>` 不得创建虚假的终端任务。`resultFuture()` 可以返回立即成功的 `Void` future，或只在带 combine 的类型化构建路径上暴露。

## 4. CompletedTaskValues 契约

- `value(ref)` 不阻塞；combine 运行时所有 member 已成功；
- ref 必须属于该 Group，且其运行时类型必须覆盖已注册类型，否则抛 `IllegalArgumentException`；
- 成功 member 的返回值可以为 null；
- 视图只在 `apply` 调用期间有效，实现可以在回调返回后释放结果引用；
- 不提供 `Map<String, Object>`，避免强转和名称重构风险；
- 不提供 future，避免 combine 重新等待、取消或编排底层任务。

combine 不是用户编写的 future 编排器，而是框架确认 join 条件后的单次业务计算。

## 5. 生命周期与结果

`submit` 分为三步：

1. 冻结 member registry 和可选 combine definition，创建 group token、member futures 与可选 terminal future；
2. 提交 members，按结构化规则处理失败、拒绝、超时和取消；
3. 所有 members 成功后构造 `CompletedTaskValues`，将 combine 作为 scoped task 提交到指定 `Par`；combine 完成后 Group 才完成。

combine 的 callable 只可在第 3 步创建和执行。它不得在 builder、`build()`、submit 的准备阶段或 member completion callback 中运行。这样它总在明确的 executor、`TaskExecutionContext` 和取消边界里执行。

空 Group 配置 combine 时，join 条件立即满足，但 combine 仍是一个提交到显式 executor 的真实任务。空 Group 无 combine 时立即成功。

`resultFuture()` 返回终端业务值；`completionFuture()` 返回完整 Group telemetry，并继续以正常 future 完成而非用异常编码 Group outcome：

| 情况 | combine | `resultFuture()` | Group outcome |
|---|---|---|---|
| 全部成功 | 执行并成功 | 成功返回 `R` | `SUCCESS` |
| member 非成功 | 不执行 | 对应失败或取消 | member 的组级 outcome |
| combine 用户失败 | 执行 | 失败 | `USER_FAILURE` |
| combine 被拒绝 | 尝试执行 | 失败 | `SUBMISSION_FAILURE` |
| 任一相关 deadline 到期 | 取决于竞态 | 超时终态 | `TIMEOUT` |
| `group.cancel()` / close | 不执行或中断 | 取消 | `GROUP_CANCELED` |

`TaskGroupResult` 需要带上 terminal 的 `TaskCompletion<?>`（或等价字段）和可空 `failedTaskName()`。combine failure 不能伪装成某个 member failure。

## 6. 取消、deadline 与观测

combine 属于 Group 的结构化子任务：

- group 取消时，未开始 combine 不得执行；运行中 combine 接收协作取消和中断请求；
- member 失败时，combine 不执行，但 terminal future 必须终态；
- combine 的失败、直消或自身超时将 Group 固定为相应 outcome；members 已终态，无 sibling 可回撤；
- combine deadline 是 `min(combine requested deadline, group deadline)`；`inheritTimeout()` 解析为 group deadline；
- Group deadline 从 submit 起计算，涵盖 fan-out 等待和 combine 运行，combine 不重新获得一整段 Group timeout；
- combine 复用 Group 的 token、scheduler 和单任务提交内核，不创建新 executor 或 timer service。

member membership 仍不生成 member-to-member 图边。但 combine 是真实多父依赖：

```text
member A ──┐
member B ──┼──> combine
member C ──┘
```

若 TaskGraph 尚不能表达 multi-parent join，第一版不得伪造 member 边或虚构 group batch。可先在 Group telemetry 中记录该关系且不参与 deadlock 环检测，或先扩展图模型后再交付 combine。combine 正常产生 `TaskCompletion` 和 TaskListener 事件；Group listener 仍只在完整结果发布后调用一次。

## 7. 非目标

本设计不提供多个 combine、部分依赖、combine 后派生任务、`dependsOn(...)`、拓扑排序、任意 DAG，或将 member failure 转成 fallback 值。它们需要完整处理 ready-set 调度、循环、部分成功、依赖失败、取消传播和图观测，不能伪装成一个 Group 便利方法。

## 8. 优点与缺点

优点：它直接表达并行获取后的组装；调用方不再编写多 future 等待和执行器切换；终端计算可取消、可观测、受 deadline 约束；`TaskRef<T>` 保留类型安全；单一全量 join 控制了 API 的扩张。

缺点：

1. **概念增多。** 用户要区分 member、listener 和 combine；仅做观测或分别消费结果时，combine 没有价值。
2. **Java 8 泛型不够自然。** `values.value(ref)` 比二元或三元函数冗长，但比 `Map<String, Object>` 更安全；语言本身无法自动从异构 refs 推导 lambda 参数列表。
3. **失败面扩大。** 组装成为可失败、拒绝、取消、超时的任务，结果、指标、测试和文档都要扩展。
4. **额外一次调度。** 显式 executor 避免污染最后完成 member 的线程，但对纯字段拼装带来排队和上下文切换开销。
5. **端到端预算更紧。** member 用掉大部分 deadline 后，combine 可能尚未开始即超时；这是正确的端到端语义，但需清晰告知用户。
6. **图模型成本。** 合法的 all-to-one join 是多父关系；若图基础设施只支持单父边，错误建模会产生错误诊断。
7. **会引出 DAG 需求。** 用户可能接着要求部分依赖或多个阶段。必须坚守一个全量、末端 combine 的限制。

## 9. 采用门槛与验收

combine 仅用于需要框架调度与观测的非平凡业务计算。只记录指标时用 Group listener；调用方需要逐项消费结果时读 member futures；需要部分结果、fallback 或多依赖节点时另行设计 workflow/DAG API。

最低验收：

1. 所有 members 成功时 combine 恰好执行一次，且能通过 `TaskRef` 无阻塞取得正确值；
2. 任一 member 非成功时 combine 不执行，terminal future 必然终态；
3. combine 在指定 executor 执行，不在 completion callback 线程执行；
4. combine 的失败、拒绝、取消、deadline 与 close 有确定 outcome；
5. Group completion 等待 terminal future，listener 只调用一次；
6. unknown ref、类型不匹配和 null 成功结果符合 `CompletedTaskValues` 契约；
7. TaskGraph 不把 membership 误作依赖，并采用选定的 multi-parent 策略；
8. 所有执行、拒绝和取消路径恢复 ThreadLocal/TTL，且不留下 pending public future。
