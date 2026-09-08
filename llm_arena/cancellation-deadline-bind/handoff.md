# Handoff：CancellationToken deadline + group 接入 bind

> 写给接手 agent 的完整交接。本会话反复中断，所有已拍板决策、语义表、实现规格集中在此。
> 先读本文件 → 再读三个代码文件 → 按「需求 1」「需求 2」实现 → 跑测试 → 更新文档与测试。

## 0. 仓库状态（接手时）

分支 `feat/parallel-task-group-builder`，**有未提交改动**（用户亲手改的）：

| 文件 | 改动 |
|---|---|
| `cancel/CancellationToken.java` | `lateBind` 改名 `bind`，链式实现重写（见 §3 现实现） |
| `scope/Par.java` | 调用点改名 |
| `cancel/*Test` ×4 | 测试改名 + `immediateVoidFuture` 占位 canceller |

另外 `mvn spotless:apply` 曾扫过仓库，额外格式化了一批**非本次语义改动**的文件（`internal/ExecutionPhaseHintFuture`、`scope/ParallelTaskGroup`、`scope/TaskGroupMemberResult`、`internal/FutureInspectorTest`、`internal/ListenableCompletionServiceTest`、`scope/ParallelTaskGroupTest`）。先跑测试再决定提交策略：

```bash
mvn test -Dtest='CancellationTokenTest,CancellationTokenLateBindRaceTest,CancellationPropagationCartesianTest,CancellationTriggerCartesianTest,CheckpointsTest,ParallelTaskGroupTest,GlobalParTest,HeuristicPurgerTest'
```

## 1. 已拍板决策（来自多轮讨论，新 agent 无此上下文，必须遵守）

1. **group 取消语义与 batch 完全一致**（结构化并发，group 原始目标之一）。旧契约 §8.2「成员主动取消不级联 siblings」**作废**。新语义：成员失败 / 成员被直接取消 / 任一 deadline，都级联取消整个 group——`Futures.allAsList` 的「取消也触发 fail-fast」行为在此拍板下恰好就是正确语义，`bind` 的 fail-fast 触发器可直接用于 group。
2. `CancellationToken` **可以依赖 Guava `ListenableFuture`** 类型/接口；不得依赖「已经创建好的组合对象」（预设 futures List、allAsList 链、submitCanceller 参数）做功能载体——但本次实现里 `bind(List, ...)` 是已接受的形态，不再推翻。
3. **submitCanceller 用 `Futures.immediateVoidFuture()` 作 NullObject**（group/自助场景无提交循环）。
4. 取消归因必须**读 token state**（state CAS 先于取消执行，无竞态窗口）；group 的 `TaskOutcome` 归因与 `TaskGroupCompletionReason` 推导都基于 token state，不得在发起取消处手写 reason。
5. 项目 0.x，只取终态最优，不考虑兼容与迁移成本；Java 8；访问器用裸 `x()` 风格；public API 用 `javax.annotation.Nullable`、内部用 checkerframework。
6. 共享内核不动：`ExecutionPhaseHintFuture`、`ScopedCallable`、`TaskExecutionContext`、`Checkpoints`、`HeuristicPurger`、`BatchExecutionContext`。

## 2. 现实现（用户重写后的 `CancellationToken.bind`，读文件确认全文）

`CancellationToken.java` 关键结构（已改）：
- 字段：`SettableFuture<Object> futureToken`、`AtomicReference<State> state`、`@Nullable CancellationToken parent`。
- 传播：构造时 `parent.futureToken.addListener(→ if parent.state().shouldInterruptCurrentThread() { CAS PROPAGATING; futureToken.cancel(true); })`。
- `bind(List<ListenableFuture<T>>, Duration, ListenableFuture<?> submitCanceller, ScheduledExecutorService)`：
  ```java
  ListenableFuture<?> listCanceller = Futures.allAsList(futures);
  FluentFuture<?> failFastFuture = FluentFuture.from(listCanceller)
          .withTimeout(timeout, timer)
          .transform(ignored -> state.compareAndSet(RUNNING, SUCCESS), directExecutor())
          .catchingAsync(Throwable.class, ex -> {
              if (ex instanceof TimeoutException) state.compareAndSet(RUNNING, TIMEOUT_CANCELED);
              else                              state.compareAndSet(RUNNING, FAIL_FAST_CANCELED);
              submitCanceller.cancel(true);
              listCanceller.cancel(true);
              return Futures.immediateCancelledFuture();
          }, directExecutor());
  futureToken.setFuture(failFastFuture);
  ```
  注意：已修好「CAS 先于 cancel」顺序 ✓；`listCanceller` 即 allAsList，cancel 它即级联成员 futures。
- `cancel()` / `cancel(boolean)` → CAS MUTUAL_CANCELED + `futureToken.cancel(interrupt)`。
- `State`：`RUNNING/SUCCESS/NO_OP(死值，可删)/FAIL_FAST_CANCELED/TIMEOUT_CANCELED/MUTUAL_CANCELED/PROPAGATING_CANCELED`。

## 3. 需求 1：CancellationToken 实现 deadline

用户原话要点：**「实现 deadline，如果有 parentToken 取 min；调用 withTimeout 时 deadline 已超，则 cancel 当前 Futures（方法可能自带支持）；注意可能取消失败——结果已算成功」**。

### 规格

1. **token 持有 deadline**：新增 `private final long deadlineNanos`（默认 `Long.MAX_VALUE`）。
   - 新构造 `CancellationToken(@Nullable parent)` → `deadlineNanos = parent == null ? MAX : parent.deadlineNanos()`（结构化 scope：子不得晚于父）。
   - 新构造 `CancellationToken(@Nullable parent, long deadlineNanos)` → `min(deadlineNanos, parent == null ? MAX : parent.deadlineNanos())` —— **「有 parentToken 去 min」落点**。
   - 访问器 `deadlineNanos()`；`remaining()`（`Duration.ofNanos(max(0, deadline - now))`）可选加。
   - 现状 `BatchExecutionContext.resolve(...)` 已自行算过 deadline（含 parent ceiling），两处 `new CancellationToken(...)` 与 `ParallelTaskGroup.buildWhileOpen` 的 `new CancellationToken(...)` 改为传入算好的 `deadlineNanos`（resolve 第 3 重载已有 `deadlineCeilingNanos/resolutionTimeNanos` 参数，正好传入）。
2. **`bind` 不再收 `Duration`**（deadline 在 token 内）：签名收敛为
   `bind(List<ListenableFuture<T>> futures, ListenableFuture<?> submitCanceller, ScheduledExecutorService timer)`；
   内部 `remaining = deadlineNanos - now`：
   - `remaining <= 0` → **同步走超时路径**，不调度：CAS `TIMEOUT_CANCELED` → `submitCanceller.cancel(true)` → `listCanceller.cancel(true)`（即取消全部 futures）→ 结束（不需要 chain/setFuture）。
   - `remaining > 0` → 现有 chain，`withTimeout(remaining, timer)`。
   - **取消失败语义**：若 futures 在 bind 调用前已成功完成，`cancel(true)` 是 no-op（Guava/`AbstractFuture` 幂等），token 仍按 deadline 判定 TIMEOUT 是**正确**的（与 Guava `withTimeout` 原生行为一致：deadline 到点即 TIMEOUT，即使部分成员已成功）。无需特判「全部成功则 SUCCESS」——除非你有更强理由，保持与原生 withTimeout 一致即可。测试里明确覆盖「已过期 bind：未完成任务被取消 / 已完成任务保持结果」。
   - 建议顺带新增 `public void timeoutCancel()`（CAS TIMEOUT_CANCELED + futureToken.cancel(true)），group 成员超时升级与「build 期间已过期同步固定 TIMEOUT」都需要外部触发词（见 §4 race 注意）。
3. `NO_OP` 顺手删除；`cancel(boolean)` 保留或删（当前生产零调用 `cancel(false)`，但测试在用；0.x 可删并改测试，量不大）。

## 4. 需求 2：group 逻辑接入 CancellationToken

### 目标形态（改写 `ParallelTaskGroup`）

删除：`cancelGroup(...)`、`markUnfinished(...)`、`cancelMembers(...)`、`timeoutMember(...)`、字段 `deadlineTimer`、`MemberState.deadlineTimer`、`start()` 里手动两层 `schedule(...)` 与 `PROPAGATING_CANCELED` 原因监听、`memberCompleted` 里的 fail-fast/timeout 分支手写 reason。
保留：`Builder`/`buildWhileOpen` 结构、`completeEmpty`、`snapshot`、`notifyListeners`、`submitPrepared`、`retainUntilComplete`。

`start(global)` 改写为纯接线：

```java
if (memberStates.isEmpty()) { completeEmpty(); return; }
ListenableFuture<?> noSubmission = Futures.immediateVoidFuture();   // §1-3 NullObject,建议提常量
// 1) 组级:组 deadline + 统一 fail-fast(失败/成员取消均级联) + 全成功 SUCCESS,一次 bind
groupToken.bind(memberFuturesSnapshot(), noSubmission, global.timeoutScheduler());
// 2) 成员级:每个成员 token 绑自己的 future(deadline 已由 token min 好)
for (MemberState member : memberStates.values()) {
    CancellationToken memberToken = member.context.batchContext().cancellationToken();
    memberToken.bind(Collections.singletonList(member.future), noSubmission, global.timeoutScheduler());
    memberToken.addCompletionListener(...);   // 见 race 注意
    member.future.addListener(() -> memberCompleted(member), directExecutor());
}
// 3) 已过期:同步固定(依赖 timeoutCancel()/构造 deadline<=now 时 bind 内部已同步超时,二选一)
```

`cancel()` / `close()` → 只剩 `groupToken.cancel()`。`buildWhileOpen` 里 groupToken 构造改为 `new CancellationToken(structuralParent == null ? null : structuralParent.cancellationToken(), groupDeadlineNanos)`；成员 token 由 `BatchExecutionContext.resolve`（第 3 重载）传入各自 `deadlineNanos`。

### 归因与 reason（读 token state，无手写取消）

`memberCompleted(member)`：只做计数 + 归因 + 收敛，不改任何取消：

| 观察（member future 终态 + token state） | member `TaskOutcome` |
|---|---|
| 成功 | `SUCCESS` |
| `ExecutionException` 且 cause 是 `SubmissionException` | `SUBMISSION_FAILURE` |
| `ExecutionException` 其他 | `USER_FAILURE`（记 failure + 若首个则记 failedMemberName） |
| future 被取消 且 member token `RUNNING`（用户直消，未触碰 token） | `MEMBER_CANCELED` |
| future 被取消 且 member token `TIMEOUT_CANCELED` | `TIMEOUT` |
| future 被取消 且 member token `PROPAGATING_CANCELED` | 读 group token：`TIMEOUT_CANCELED`→`TIMEOUT`；`FAIL_FAST_CANCELED`→`FAIL_FAST`；其余→`GROUP_CANCELED` |

`convergeIfTerminal()` reason 推导（取代 `completionReason` 手写 CAS）：
- `groupToken.state()==TIMEOUT_CANCELED` → `TIMEOUT`
- `FAIL_FAST_CANCELED` → 有记录到的失败成员（failedMemberName != null）→ `FAILED`；无失败成员（触发源是某成员被直接取消）→ `CANCELED`
- `MUTUAL_CANCELED` / `PROPAGATING_CANCELED` → `CANCELED`
- 全成员 `SUCCESS` → `SUCCESS`；其余（如个别 MEMBER_CANCELED 但组 token 仍 RUNNING——理论不发生，级联会在首成员取消时触发）兜底 `CANCELED`

### Race 注意（member timeout 升级为组 TIMEOUT，最容易写错）

成员自己的 deadline 触发时，级联顺序是：member token CAS `TIMEOUT_CANCELED` → **member futures 被取消** → group 的 `allAsList` 看到取消 → group CAS `FAIL_FAST_CANCELED`。若升级动作（`memberToken` 状态监听 → `groupToken.timeoutCancel()`）挂在 `addCompletionListener`（futureToken 完成才触发），**必然晚于** group 级联（同线程 directExecutor 内联链），group 会错误地停在 `FAIL_FAST_CANCELED`（组 reason FAILED，契约要求 TIMEOUT）。

两个可行解，选一：
- **A（推荐）**：token 增加「状态监听在 CAS 之后、取消 action 之前同步触发」的能力（`addStateListener(Consumer<State>)`，`bind` 的 `catchingAsync`/`timeoutCancel` 里 CAS 后立即通知）。group 在 member token 上注册：`state == TIMEOUT_CANCELED → groupToken.timeoutCancel()`。顺序：member CAS TIMEOUT → 监听器同步跑 → group 先 CAS TIMEOUT → 之后 group 一切级联的 FAIL_FAST CAS 失败 → 组 reason TIMEOUT ✓。
- B：不做升级监听；group reason 推导时若「组 token FAIL_FAST_CANCELED 且至少一个成员 reason==TIMEOUT 且无失败成员」→ 映射 `TIMEOUT`。简单但归因与触发错位，不推荐。

### 影响面

- `ParallelTaskGroupTest`：成员主动取消相关用例按新语义（级联、reason 变化）改；deadline/超时归因用例按归因表校准。契约 §8.2 测试原样会挂，属预期。
- `CancellationTokenTest` 等 4 个改名测试补 deadline 用例（parent min、expired-at-bind、cancel-fails-on-done-future）。
- 文档：`design/task-group-cancellation.md` §8.2/§8.4 按新语义改写；user-guide 取消章节；migration-v0.2 记 batch 元素取消时 token state 由 `FAIL_FAST_CANCELED` 概念迁移的说明（如采纳）；AGENTS.md 里「lateBind wires...」invariant 改名与改写。
- 全量 `mvn test` + `mvn spotless:apply` 收尾。
