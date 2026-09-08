# TaskGroup 设计契约：监听、观测与验收

> 本文是 TaskGroup 设计契约系列之一（由原《独立并行任务组最终设计契约》按章节拆分）。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。

## 10. TaskListener 与 Group telemetry

### 10.1 成员级 TaskListener

真正进入 `ScopedCallable.call()` 的成员继续触发现有 `TaskListener.TaskEvent`：

- 成功结果、用户异常；
- submit/start/end timing；
- queue wait 分类；
- `TaskContext` 和 Batch taskName。

执行前取消或提交失败的成员没有真实 start/end，不得伪造 TaskEvent。它们必须在 `TaskGroupResult` 和 Group listener 中可见。

Group 通过 MemberState 中保存的 `TaskExecutionContext` 身份把 memberName 与 TaskEvent/结果关联，不需要新增 current group context。首版不要求修改 `TaskContext` 增加 groupId/memberName。

### 10.2 TaskGroupListener

```java
@FunctionalInterface
public interface TaskGroupListener {
    void onTaskGroupComplete(TaskGroupEvent event);
}
```

`TaskGroupEvent` 可以直接包装不可变 `TaskGroupResult`，或暴露等价字段。它只在 CLOSED 后调用一次。

要求：

- listener 异常隔离并通过 JUL 记录；
- 不改变 completion result；
- listener 回调期间不得安装任何 member current task 或 group current context；
- 回调不在 Group lock 内执行；
- 顺序固定为先固定 result/completion future，再调用 listener，避免 listener 阻塞调用方观察终态；
- 文档要求 listener 非阻塞并容忍并发，但框架仍必须保护自身状态。

## 11. TaskGraph 规则

Group membership 不是依赖关系：

```text
group
├─ member A
├─ member B
└─ member C
```

MUST NOT 产生：

```text
A -> B
B -> C
fake-group-batch -> A/B/C
```

规则：

- 请求线程创建的 Group，其 members 不因同组而写 TaskGraph edge；
- scoped task 内创建的 Group，可以为真实 outer batch 到各 member batch 写边；
- member callable 内部调用 `Par.map()` 时，现有 `TaskExecutionContext.current()` 使该 Batch 成为 member 的真实结构化 child；
- groupId/memberName 的 membership 只写入 Group telemetry；
- 相同物理 executor 的成员不会仅因同组产生 self-loop/deadlock 告警。

## 13. 并发不变量

实现和测试必须证明：

1. 每个 memberName 在一个 `TaskGroupSpec` 中至多定义一次；
2. 每次 submit 中每个 callable 最多执行一次；
3. spec 不可变、可复用；每次 submit 冻结的是同一份完整定义；
4. Group 一旦发布，其 registry 完整、不可扩展，且每个成员都拥有一个最终终态的公开 future；
5. executor rejection 不能留下 pending future；
6. `terminalCount` 对每个成员最多增加一次；
7. Group completion reason 只固定一次；
8. Group listener 只调用一次；
9. completion future 只在全部冻结成员终态后完成；
10. 取消赢得 execution claim 时用户 callable 不执行；
11. 任一 inline 执行前，全部成员已经出现在 registry；
12. 所有 ThreadLocal/TTL 在正常、异常、取消、拒绝和 inline 路径上恢复；
13. Group lock 内不执行用户 callable、listener、future cancellation 或 executor 方法；
14. 同一成员的公开 future 状态、`TaskOutcome`（成员结果 `outcome()`）、Group result 三者一致。

## 14. 必测矩阵

### 14.1 基本行为

1. 三个异构成员分别返回不同类型且各执行一次；
2. 成员使用不同 `Par`/executor 时并行运行；
3. 同一 `Par` 上多个成员均独立提交，不经过滑动窗口；
4. 重复 memberName 与 null 参数在 `task()` 配置期拒绝，空白/null 名称在 `TaskRef` 构造期拒绝；未知 executorName 在 submit 时拒绝且没有 callable 执行；`future(ref)` 拒绝 raw 结果类型不覆盖注册类型的令牌；
5. `task()` 不创建执行上下文、不启动 timer、不提交或执行 callable；
6. 空 spec submit 返回立即 SUCCESS 的 Group，未创建 timer；`TaskRef` 在 submit 后即可经 `group.future(ref)` 稳定解析。

### 14.2 submit 与关闭竞态

7. 同一个 spec 可重复 submit，每次产生独立 Group（不同 groupId）；
8. `TaskGroup.submit()` 与 `GlobalPar.close()` 竞争时，要么完整组被接纳，要么 submit 完整拒绝，绝无部分 admission；
9. direct executor/inline fallback 中，任一 callable 执行前完整成员集合已可查询；
10. executor rejection 产生 SUBMISSION_FAILURE、触发 fail-fast、所有 future 终态；
11. 已注册但尚未 `executor.execute()` 的成员被 fail-fast/cancel 后不得运行 callable；
12. 重复 cancel、close 不重复计数或回调；submit 提交循环中的 inline failure 会终结尚未提交成员。

### 14.3 取消与原因

13. 手动 group.cancel 使未完成成员记录 GROUP_CANCELED；
14. 直接 member future.cancel 级联取消 Group：该成员 `MEMBER_CANCELED`，未完成 siblings `GROUP_CANCELED`，Group reason `CANCELED`；
15. 成员失败固定 FAILED，first failedMemberName 稳定，siblings 为 FAIL_FAST；
16. 取消发生在 SUBMITTED/RUNNING/TERMINAL 三个阶段时状态一致；
17. 用户忽略中断时公开 future 可以先取消，但 Group 仍能按公开 future 终态完成；
18. outer scoped task 取消向 group/member 传播且不反向影响 outer。

### 14.4 deadline

19. group deadline 早于 member deadline，Group TIMEOUT，成员 TIMEOUT；
20. member deadline 早于 group deadline，该 member TIMEOUT 并使 Group TIMEOUT；
21. failure、timeout、cancel 同时竞争时 first-wins 且不覆盖；
22. 外层 deadline 在 submit 准备期间过期：所有冻结成员不执行，Group TIMEOUT；
23. Group 提前完成后 timer 取消或触发为 no-op。

### 14.5 上下文与监控

24. 每个运行成员看到自己的 `TaskExecutionContext.current()`；执行后恢复 previous/null；
25. inline 嵌套执行呈现 outer -> member -> outer；
26. `SubmissionScope` 仅覆盖 executor submission，并在 rejection/inline/异常后恢复；
27. TaskListener 中 current task 为 null，TaskEvent 指向正确成员 TaskContext；
28. 执行前取消和 submission failure 不产生虚假 TaskEvent；
29. Group listener 只调用一次，异常不改变结果；
30. TTL 值以 `submit()` 的 prepare 阶段为捕获时点传播并恢复；`task()` 时的值不构成快照，普通 ThreadLocal 不承诺传播。

### 14.6 TaskGraph 与关闭

31. 请求线程 Group 不产生 membership dependency edge；
32. outer scoped task 创建 Group 时，只产生 outer->member 的真实边；
33. member 内嵌套 `Par.map()` 产生 member->child Batch 的真实边；
34. 同一 executor 的 siblings 不因 membership 产生 self-loop；
35. GlobalPar close 后拒绝新 submit；先于 close 完成 admission 的完整 Group 可继续运行；
36. close 前完整冻结的成员继续走终止、timeout 和 telemetry；
37. Group close 不关闭任何注册 executor。

## 15. 验收标准

验收时必须同时满足：

- 公共 API、状态机和结果原因符合本文；
- 没有新增 current-group ThreadLocal/TTL 或虚构 Group Batch；
- Group 与 Batch 共享单任务运行内核（`TaskSubmissions`），没有复制取消/phase/ScopedCallable 逻辑；
- submit admission、cancel、run、rejection、timeout 的竞态均有确定且测试覆盖的结果；
- 所有冻结 public future 必然终态；
- TaskGraph 只记录真实结构化依赖；
- 全量 Maven 测试通过且 Java 8 main source 兼容；
- 用户文档说明 spec/submit、`TaskRef`、close、cancel、deadline、成员取消和 observation 生命周期。
