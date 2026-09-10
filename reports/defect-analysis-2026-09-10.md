# 缺陷分析报告（2026-09-10，0.2.0 快照）

范围：`dev/v0.3.0` 起点 = `main` @ `beae359`（`0415ef7..beae359` 仅含 `.github/workflows/release.yml`，
被分析的 `src/` 与 `design/` 与 `0415ef7` 完全一致；最新源码提交为 `4a941c2` queue 与 `e0e8849` 根包合并 +
TaskGroup/combine）。
方法：静态审读 + 可执行探针。**本文所有结论均由分析者在工作区临时探针中独立复现**（探针跑完即删，
最小复现代码见各条），不是静态推理。基线 `mvn test` = 512 tests, 0 failures, 0 errors——以下问题
全部位于现有测试未覆盖的路径上。

| # | 位置 | 影响 | 严重度 | 状态 |
|---|---|---|---|---|
| 1 | `queue/VariableLinkedBlockingQueue.java:427-437` | 抛异常的 target 永久损坏队列（count 漂移、后续 poll/take 抛 NPE、元素丢失） | P1 | 新发现 |
| 2 | `queue/VariableLinkedBlockingQueue.java:404,435` | 缩容后 `clear()`/`drainTo()` 不唤醒阻塞的 `put`，生产者永久挂起 | P1 | **已知**，已决定 0.2.0 带缺陷发布、0.2.1 修复 |
| 3 | `SlidingWindowSubmitter.java:100-109,167` + `Task.java:96-100` | 元素被报 `SUBMISSION_FAILURE`，但用户 callable 实际已执行 | P1 | 新发现 |
| 4 | `CancellationToken.java:133-139` | 已过期 deadline 不同步提交 `TIMEOUT`，用户 callable 仍会进入（违反契约 MUST） | P2 | 新发现 |
| 5 | `TaskGroup.java:290-299,362-380` | 失败成员的组 outcome 随完成顺序在 `USER_FAILURE`/`MEMBER_CANCELED` 间漂移 | P2 | 已知形态（被测试与文档固化），需决策 |

---

## 验证：可执行复现（`verification/defect-repro/`）

```bash
./verification/defect-repro/run.sh    # 仅用公开 API，退出码 0 = 五条全部复现
```

harness 在 `beae359` 上运行，**exit 0**（五条全部复现；退出码非 0 表示某条未能复现，即报告需要修正）：

```text
revision: beae359

=== CHECK 1: VariableLinkedBlockingQueue.drainTo is not exception-safe ===
after drainTo : raised=java.lang.IllegalStateException: target refuses 1 size=2 isEmpty=false  (chain holds 1 element)
poll#1        : 2
poll#2        : NullPointerException: null
final         : size=1 isEmpty=false

=== CHECK 2: producer stays parked after setCapacity shrink + clear/drainTo ===
clear        : queueSize=0 remainingCapacity=1 producerStillParked=true producerWrote=false
drainTo        : queueSize=0 remainingCapacity=1 producerStillParked=true producerWrote=false

=== CHECK 3: batch element reported SUBMISSION_FAILURE after its callable ran ===
reachedHandoff=true cancelReturned=true userCallableRan=true element1Outcome=SUCCESS
  element0 outcome=SUCCESS value=1
  element1 outcome=SUCCESS value=2
  element2 outcome=SUBMISSION_FAILURE value=ExecutionException: SubmissionException: Task submission failed
over 10 rounds: 1 contradiction (callable ran, caller sees SUBMISSION_FAILURE), 8 canceled-before-run, 1 bound-as-SUCCESS

=== CHECK 4: group deadline already expired at submit, member callable still runs ===
firstRound    : groupOutcome=SUCCESS memberOutcome=SUCCESS memberCallableRan=true
over 20 rounds: memberCallableRan in 20 rounds, group outcomes=[SUCCESS]

=== CHECK 5: group outcome for a failing member depends on completion order ===
single failing member   : outcome=MEMBER_CANCELED failedTask=boom member=USER_FAILURE
failure completes first : outcome=USER_FAILURE failedTask=boom member=USER_FAILURE
failure completes last  : outcome=MEMBER_CANCELED failedTask=boom-late member=USER_FAILURE

=== CONTROL: healthy group with a terminal combine still succeeds ===
outcome=SUCCESS combine=SUCCESS

=== SUMMARY (REPRODUCED = the reported defect was observed) ===
1 drainTo-throwing-target: REPRODUCED — size stays 1 and the second poll throws NPE after one element is lost
2 shrink-lost-wakeup: REPRODUCED — producer remains parked although the queue is empty and has capacity again
3 submitter-handoff-race: REPRODUCED — the abandon/bind race produced a caller-visible SUBMISSION_FAILURE for a task that ran its callable in 1/10 rounds
4 expired-deadline-runs-code: REPRODUCED — callable entered user code in 20/20 rounds although the deadline had already expired before submission
5 group-outcome-order-dependence: REPRODUCED — the same failure reads MEMBER_CANCELED when it completes last and USER_FAILURE when it completes first
control healthy-group-with-combine: OK — harness reports healthy paths as healthy
exit 0: every reported defect reproduced
```

**两处比前文初稿更精确的观察**（已按此更新第 3、4 节）：

- **第 3 条是竞态，不是必现**：取消级联（token bind 触发 `FAIL_FAST` → 取消元素 future）通常在提交线程跑起任务之前就取消了引擎 future（8/10 轮），矛盾只在提交线程抢在级联之前执行任务的轮次出现（本轮 1/10、上一轮 2/10）。内部层面直接驱动 `SlidingWindowSubmitter`（无后续 token bind）时可 100% 复现；公开 API 路径的暴露率取决于 executor 的 `execute` 行为与线程调度。
- **第 4 条比初稿更严重**：本轮 20/20 轮组 outcome 是 `SUCCESS`（而非初稿观察到的 `TIMEOUT`）——一个 deadline 在提交前就已过期的组既进入了全部成员的用户代码，又报告成功；`TIMEOUT`/`SUCCESS` 之间的差异由"0 延迟 timer 与成员完成"的竞态决定。

## 1. `VariableLinkedBlockingQueue.drainTo` 不是异常安全的（新发现，P1）

**位置**：`src/main/java/io/github/monadrome/parallelinscope/queue/VariableLinkedBlockingQueue.java:427-437`

```java
while (i < n) {
    E x = dequeue();   // 先摘链、item 置 null
    c.add(x);          // 可能抛
    ++i;
}
...
} finally {
    if (i > 0) {
        signalNotFull = (count.getAndAdd(-i) == capacity);
    }
}
```

**机制**：循环是"先摘链再 add"，但 `finally` 只按 `i`（成功 add 的个数）回退 `count`。第一次 `add`
就抛时 `i == 0`：节点已摘、元素已从链上消失，`count` 却不减——链长与 `count` 永久相差 1。JDK 的
`LinkedBlockingQueue.drainTo` 是"先 `c.add(p.item)` 再摘链 + finally 恢复 `head`"，因此不存在该状态；
本类在移植时改了顺序，丢掉了异常安全。

**最小复现**（队列 `[1,2]`，`target.add` 抛 `IllegalStateException`）：

```java
VariableLinkedBlockingQueue<Integer> q = new VariableLinkedBlockingQueue<>(5);
q.offer(1); q.offer(2);
Collection<Integer> throwing = new ArrayList<Integer>() {
    @Override public boolean add(Integer e) { throw new IllegalStateException("target refuses " + e); }
};
try { q.drainTo(throwing); } catch (Throwable ignored) { }
q.size(); q.isEmpty(); q.poll(); q.poll();
```

**实测输出**：

```text
after drainTo: raised=java.lang.IllegalStateException: target refuses 1  size=2  isEmpty=false   ← 链上其实只剩 1 个
poll#1=2
poll#2=NullPointerException: Cannot read field "item" because "first" is null                     ← dequeue() 的 requireNonNull
final size=1  isEmpty=false                                                                       ← count 永久卡住
```

**后果**：元素 1 丢失且已传给 target（无法回滚，javadoc 明确不承诺回滚）；此后 `count` 永远比链长多 1，
每次 `poll()`/`take()` 都在 `dequeue()` 抛 NPE（`poll()` 的快路径先看 `count == 0`，count=1 所以会进
dequeue），`isEmpty()` 永远为 false。触发条件现实：target 是有界集合或带校验的集合（`add` 抛
`IllegalStateException`）。

**为什么测试没抓到**：`VariableLinkedBlockingQueueTest`/`CartesianTest` 的 `drainTo` 用例全部使用
`ArrayList` 作 target，没有"会抛的 target"用例。

**修复建议**（照 JDK 形状改写，天然同时修掉 #2 的谓词）：

```java
Node<E> h = head;
int i = 0;
try {
    while (i < n) {
        Node<E> p = h.next;
        c.add(p.item);        // 先 add：target 抛时队列不变
        p.item = null;
        h.next = h;           // 摘链
        h = p;
        ++i;
    }
    return n;
} finally {
    if (i > 0) {
        head = h;
        signalNotFull = (count.getAndAdd(-i) >= capacity);   // 见 #2
    }
}
```

**回归测试**：对两个 `drainTo` 重载各加一条"target 在第一个/第二个元素抛异常"的用例，断言
`size()`、`isEmpty()`、剩余元素顺序、以及后续 `poll()/take()` 不抛 NPE。

---

## 2. 缩容路径丢失 `notFull` 唤醒（已知缺陷，0.2.1 计划修复）

**位置**：`VariableLinkedBlockingQueue.java:404`（`clear`）与 `:435`（`drainTo`）

**机制**：等待谓词在 0.2.0 的准入加固中改成 `count >= capacity`（`:189/:210/:228`），且
`setCapacity()` 允许缩容到当前 size 以下（`setCapacity_shrink_belowSize_keepsExistingElements` 固化了
"缩容保留既有元素"），因此 `count > capacity` 是合法状态。但 `clear()`/`drainTo()` 仍沿用 JDK 的
"队列曾被填满"判定 `count == capacity`，在 `count > capacity` 时**不会**发 `notFull.signal()`。

**最小复现**（capacity=2 → 塞满 → 缩到 1 → 生产者 `put(3)` 阻塞 → `clear()`）：

```text
clear-after-shrink producerAlive=true completed=false size=0 remainingCapacity=1
drainTo-after-shrink drained=2 size=0 remainingCapacity=1 producerAlive=true completed=false
```

队列已空、容量已可用，生产者却永久 park；后续 `put` 也因 `c + 1 < capacity` 不再向下传递信号。

**修复建议**：

- `clear()`：`if (count.getAndSet(0) >= capacity) notFull.signal();`（clear 后必为 0 < capacity，`>=` 即精确的跨越判定）
- `drainTo()`：`int before = count.getAndAdd(-i); signalNotFull = before >= capacity && before - i < capacity;`（或简化为 `before >= capacity`，多唤一个是无害的，被唤醒者会重查谓词）
- `take()`/`poll()`/`unlink()` 的单次递减保持 `==`：单步递减下 `c == capacity` 恰是"从满到不满"的跨越条件，不要一起改。

**测试缺口**：`clearOnFullQueueReleasesBlockedProducer`、`twoBlockedPutters_bothCompleteAfterClear/AfterDrain`
都只在 `count == capacity` 下停等，没有"缩容 + 停等"的组合用例。建议回归测试：park 一个生产者 →
`setCapacity(缩到 size 以下)` → `clear()`/`drainTo()` 清空 → 断言生产者被唤醒并成功写入。

> 说明：本项在 2026-09-10 的 0.2.0 发布决策中已被确认为已知缺陷（非回归：对已发布的
> `io.github.huatalk:parallel-in-scope:0.1.0` 编译同一复现同样挂起），决定 0.2.1 修复。本报告的增量是
> `drainTo()` 路径的同一复现与精确修复谓词。另注：`take()` 的 `==` 谓词本身是正确的（见上），
> 不构成第三个受害点。

---

## 3. 提交取消与 executor 交接的竞态：用户代码跑了，调用方看到 `SUBMISSION_FAILURE`（新发现，P1）

**位置**：`SlidingWindowSubmitter.java:100-109`（取消监听以 `nextIndex.get()` 为 abandon 起点）、
`:167`（`result.get(index).bind(fallbackSubmit(...))`）、`Task.java:96-100`（`bind`）

**机制**：async 提交循环在 `fallbackSubmit`（已把任务交给 executor）与 `bind`（把真实 future 接到
placeholder 上）之间有一个窗口。此时调用方 `submitCanceller.cancel(true)`，directExecutor 监听立即在
取消线程上执行 `abandonRemaining(results, nextIndex.get=index, ...)`，把**正在提交的那个** placeholder
用 `setException(SubmissionException)` 终结。随后 loop 线程执行 `placeholder.setFuture(real)`：Guava 只在
placeholder 已被 *cancelled* 时才取消 delegate，`setException` 路径不会，于是 `real`（引擎 future）继续
执行用户 callable。`Task.abandon(null)`（纯 cancel）路径是安全的，只有带异常因的 abandon 有这个问题。

**最小复现**（`verification/defect-repro` CHECK 3：`parallelism=1`、3 个元素的批次，executor 第 2 次
`execute` 阻塞并发出闩信号，元素 1 为用户代码探针；`cancel(true)` 中断提交线程后，提交线程与取消监听
竞争——前者跑任务并 `bind`，后者 abandon 在飞行中的 placeholder）：

```text
reachedHandoff=true cancelReturned=true userCallableRan=true element1Outcome=SUBMISSION_FAILURE
over 10 rounds: 1 contradiction (callable ran, caller sees SUBMISSION_FAILURE),
                8 canceled-before-run, 1 bound-as-SUCCESS
```

内部层面（直接驱动 `SlidingWindowSubmitter`、没有 `Par.map` 随后执行的 token bind）可 100% 复现
（`userCodeRan=true, outcome=SUBMISSION_FAILURE, value=null`）；公开 API 路径下 8/10 轮由取消级联先
取消引擎 future（`canceled-before-run`，不产生矛盾），矛盾出现在提交线程抢在级联之前执行任务的轮次。

**后果**：调用方被告知该元素"从未提交"（值 null、outcome 为提交失败），实际用户 callable 已执行、副作用
已发生；按该 outcome 重试即重复执行。这与 `TaskBatchResult` 的契约表述（"unsubmitted placeholders fail
with the interruption cause"）矛盾。

**为什么测试没抓到**：`SlidingWindowSubmitterTest#cancellingSubmitterAbandonsRemainingPlaceholders` 只在
loop 停在 `blockingQueue.take()` 时取消，且对每个 result 只断言 `isDone()`，同时容忍 failure 与 cancel。

**修复建议**：

1. 主修：**claim-then-submit**——把 `nextIndex` 的推进放到 `fallbackSubmit` 之前（即"已认领"的索引不再
   被 abandon 覆盖），使"已交给 executor 的元素"不再被当作未提交元素终结；同时明确语义：进入
   `executor.execute` 之后该元素只能通过 future 本身取消。
2. 兜底：`Task.bind` 中 `if (!placeholder.setFuture(real)) real.cancel(true);`——placeholder 已被终结时
   取消真实 future，避免"报失败但仍在跑"的不一致（注意此时用户代码可能已部分执行，属于尽力而为）。
3. 若选择只做 2，需接受窗口内用户代码可能已经开始的语义；建议 1+2 一起。

**回归测试**：用"第 N 次 `execute` 阻塞"的 executor 固定交错，断言（a）元素判定为提交失败时其 callable
未执行，或（b）若已执行则其 future 反映真实结果，二者必居其一且可复现。

---

## 4. 已过期 deadline 不同步提交 `TIMEOUT`，用户 callable 仍会进入（新发现，P2）

**位置**：`CancellationToken.java:133-139`

```java
if (deadlineNanos != Long.MAX_VALUE) {
    failFastFuture = failFastFuture.withTimeout(
            Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime())), timer);
}
```

**机制**：deadline 已经过期（`deadlineNanos <= now`）时，只是把 0 延迟超时**排到** `timeoutActionPool`
（`GlobalPar` 的 cached pool），状态并不立即提交。`bind` 返回后 token 仍是 `RUNNING`，于是
`TaskGroup.submit()` 的 `start()` → `submitPrepared()` 提交循环可以赢得这场竞态，成员进入用户 callable。
`Checkpoints.checkpoint` 只读 token 状态、不比对 deadline，因此没有第二道防线。

**最小复现**（`verification/defect-repro` CHECK 4：`Duration.ofNanos(1)` 的组 deadline 在成员提交前即已过期，
成员 `inheritTimeout()`，direct executor）：

```text
first round: groupOutcome=SUCCESS memberOutcome=SUCCESS memberCallableRan=true
over 20 rounds: memberCallableRan in 20 rounds, group outcomes=[SUCCESS]
```

稳定部分是"用户代码必然进入"（20/20 轮）；组 outcome 是竞态：本轮 20/20 报 `SUCCESS`（0 延迟 timer 尚未
被 action pool 线程执行，成员已完成），早前一轮探针报 `TIMEOUT`。两者都违反契约——契约要求此时固定
`TIMEOUT`、取消全部成员且不得进入用户 callable；报 `SUCCESS` 的形态更糟：一个 deadline 早已过期的组
既执行了全部用户代码，又自称成功。

**契约依据**：`design/task-group-cancellation.md` §8.4「若外层 deadline 在 submit 准备期间已经到期……
不得进入用户 callable」与必测矩阵 #22。

**真实触发窗口**：外层或自身 deadline 已过、但其超时动作尚未被 action pool 线程执行时（scheduler 线程
忙、GC、cached pool 线程创建延迟）。探针用 ns 级 timeout 把它变成必现；direct executor 下必现。

**修复建议**：在 `bind` 中先判定过期——若 `deadlineNanos - System.nanoTime() <= 0`，则**同步**
`transitionTo(TIMEOUT)` 并取消已绑定 futures（等价于超时动作立即执行完毕），而不是排 0 延迟 timer；
未过期才走 `withTimeout`。修复后 `TaskGroup` 侧无需改动：`submitPrepared()` 已有 `if (!future.isDone())`
跳过，空组 + combine 的 `submitTerminalOnce()` 也有 `isDone` 判断。

**边界说明**：Batch 路径（`Par.map` → `submitAll` → `bind`）是**先提交后 bind**，bind 时刻的同步提交
无法阻止已提交元素执行；若要满足矩阵 #22 对 batch 的同类要求，需要另行把 deadline 判定前移到提交前。
本报告只要求 Group 路径符合契约。

**回归测试**：构造"提交时 deadline 已过期"的组（ns 级 timeout 或注入已过期的外层 deadline），断言
成员 callable 未执行、组与成员均为 `TIMEOUT`、且无 pending future。

---

## 5. 失败成员的组 outcome 随完成顺序漂移（已知形态，需决策，P2）

**位置**：`TaskGroup.java:290-299`（只有 terminal combine 同步 `failFastCancel()`）、`:362-380`（`deriveOutcome`）

**机制**：成员失败只依赖 group bind 的异步回调提交 `FAIL_FAST`。若失败成员是**最后一个**到达终态的
（单成员组必然如此），`convergeIfTerminal()` 会读到仍 `RUNNING` 的 group token，`deriveOutcome()` 的
`SUCCESS`/`RUNNING` 分支只问"是否全成功"，于是返回 `MEMBER_CANCELED`——尽管 `failedTaskName` 已记录、
成员快照是 `USER_FAILURE`。

**实测**：

```text
单成员组抛异常        → outcome=MEMBER_CANCELED, failedTaskName=boom, member=USER_FAILURE, failure=IllegalStateException
失败者最后完成         → outcome=MEMBER_CANCELED, failedTaskName=slow-boom, fast=SUCCESS, slowBoom=USER_FAILURE
失败者先完成           → outcome=USER_FAILURE
同样失败 + 有 combine  → outcome=USER_FAILURE
```

**判定**：同一逻辑两种结果 = 竞态泄漏到公开 API；并与三份契约冲突（`task-group-api-and-options` §3.3
「fail-fast 时组沿用失败成员自己的 outcome」、`task-group-cancellation` §8.4 归因映射、
`task-group-terminal-combine` §7 表格）。但它被 `ScopedTaskContractTest.java:90-96` 明确断言、
被 `design/task-group-lifecycle.md:240-241` 写为规则，因此修改属**行为变更**，必须实现 + 测试 + 文档
一次性同步。

**修复建议（推荐窄改）**：`deriveOutcome()` 的 `SUCCESS`/`RUNNING` 分支优先采用已记录的失败者：

```java
case SUCCESS:
case RUNNING:
    MemberState recordedFailure = failedTask();   // failedTaskName 已记录则返回该成员/terminal
    if (recordedFailure != null) return recordedFailure.reason;
    ... // 原 allSuccess 判定
```

`FAIL_FAST` 分支可复用同一个 `failedTask()` 私有方法。该改法不动 token 提交时序，因而不会改变
"失败 vs 超时"的 first-wins 竞态。备选方案是"任何成员失败都同步 `groupToken.failFastCancel()`"（与
combine 对称），但会把 FAIL_FAST 提交提前到成员观测点，可能改变 mixed 场景的 first-wins 结果，风险更高。

**需同步修改**：`ScopedTaskContractTest.java:90-96`（断言与注释）、`design/task-group-lifecycle.md:240-241`
（RUNNING 收敛规则），并核对 `task-group-api-and-options.md` §3.3、`task-group-cancellation.md` §8.4/§8.4.1、
`task-group-terminal-combine.md` §7 的表述一致性。

**验证记录**：该修复曾在工作区临时应用并跑完全量套件——4 个探针全部转为一致的 `USER_FAILURE`，
全量仅 `ScopedTaskContractTest.userExceptionIsReportedAsUserFailure[2]` 失败（正是固化旧行为的断言），
其余 511 项通过。改动已还原，未提交。

---

## 已排查、未发现缺陷的区域

静态审读 + 探针（子代理深挖后判定无缺陷；其中两条队列缺陷与两条内核缺陷已被复现，故该列表有可信度）：

- `HeuristicPurger`：IDLE/BUSY CAS 保证单执行者；BUSY 期间丢弃的信号由运行期读取 `claimThrough` 与
  finally 重评估覆盖；`settleThrough` 单调；expiry 只结算更早 sequence。**注**：`adr/0003:66-70` 说失败
  的 purge 允许同一 sequence 重试，代码与 `HeuristicPurgerConcurrencyTest` 要求新信号才重试——ADR 措辞
  过时，非代码缺陷。
- `ActionGate`：全程 synchronized，边界只消费一次，连续 open 满足 minInvocations/minInterval。
- `SmartBlockingQueue`：`offer()` 是 TPE 唯一入口；`capacity()` 在 `capacityOf`/`hasFiniteCapacity` 正确特判。
- `GlobalPar`：admission/close/retain 线性化与计数（探针：5/5 轮无挂起、无计数泄漏）；`GlobalPar.close()`
  与 combine 的 join 竞争后仍正常提交并终态。
- `ExecutionPhaseHintFuture`：claim/interrupt 竞态由 phase 与 monitor 的总序封闭，无丢失中断窗口。
- `CancellationToken`：cancel-before-bind、`allAsList` vs `successfulAsList` 的角色、commit-before-cancel 顺序。
- `TaskGraphObservationScope`：install/restore 平衡；rejection/inline/异常路径无 pending public future；
  `ScopedCallable`/`SubmissionScope` 无 TTL 恢复漏洞。
- combine 路径（本次新功能）：TTL/结构父/`TaskExecutionContext` 在 submit 准备阶段捕获、被拒不 inline、
  直消/运行中取消/close/自身 deadline 升级等路径均符合设计。

## 建议顺序

1. #1 与 #3 是新发现的 P1，且都是"调用方看到的状态与真实执行不一致"这一类，建议优先并各自带回归测试。
2. #2 按既有决定随 0.2.1 处理（与 #1 在同一段代码，建议同一次改完，避免二次改动同一方法）。
3. #4 是内核时序修复，范围小但触及 `CancellationToken`，建议单独一次改动 + 专门的过期 deadline 用例。
4. #5 需先决策语义，再实现 + 测试 + 文档同步。

---

## 附录 A：#5 若实施，需一并修改的位置（行号已核对）

**实现（同一次改动）**

1. `TaskGroup.java:362-376`（`deriveOutcome`）—— `SUCCESS`/`RUNNING` 分支在 `allSuccess` 判定前插入
   `failedTaskName != null` 的优先返回，并复用 `FAIL_FAST` 分支（`:366-369`）的"失败名可能是 terminal
   combine"解析。
2. `TaskGroup.java:355-361`（`deriveOutcome` javadoc）—— 补一句"已记录的失败优先于 RUNNING/SUCCESS 的全成功规则"。
3. `TaskGroup.java:289-295`（`memberCompleted` 注释）—— 删掉"lone member failure 在 RUNNING token 下读作
   MEMBER_CANCELED"的表述；terminal 专用 `failFastCancel()` 的理由（`:288-291`）仍成立，保留。

**测试**

4. `ScopedTaskContractTest.java:92-99` —— `:96` 断言改为 `USER_FAILURE`，`:94-95` 注释重写。
5. `TaskGroupTest.java:392-398` —— `:398` 的 `isIn(TIMEOUT, MEMBER_CANCELED)` 需接纳"已记录失败"的 reason；
   先确认该用例里成员实际记录的是 TIMEOUT 还是 USER_FAILURE 再收窄（若成员仍记 TIMEOUT，则
   `failedTaskName` 保持 null，MEMBER_CANCELED 不再是合法期望值）。
6. 新增回归用例（跨顺序一致性）：单成员失败（同 `ScopedTaskContractTest` 形状）、失败者最后完成
   （同 `TaskGroupTest:155-176` 形状）都必须读 `USER_FAILURE`；补一个 lone `SUBMISSION_FAILURE` 变体。
7. 判定为**无需改动**：`TaskGroupTest:171-174/:253/:508-510/:985-986/:1055-1056`、
   `TaskGroupCombineTest:104/130/200/241/269/292`、`TaskFutureTest:223/229/:499/515`、
   `TaskCompletionTest:76/82`、`FutureInspectorTest:29/53`、`TaskBatchResultTest`、`TaskGroupOptionsTest`、
   三个 Cartesian 测试。

**契约文档**

8. `design/task-group-lifecycle.md:240-241` —— 重写该条：优先采用已记录失败任务的 outcome，仅当无失败记录
   时才记 `MEMBER_CANCELED`（§6 完成原因表 `:224-231` 不变）。
9. `design/task-group-cancellation.md:102-103` —— 同一句话的组级归因段落。
10. `design/task-group-cancellation.md:131-132` —— §8.4.1 分叉说明需点名"新的失败优先拦截"。
11. `design/task-group-terminal-combine.md:161` —— 删除引用"lone member 失败在 RUNNING token 下收敛为
    MEMBER_CANCELED"的括注；combine 同步 `failFastCancel()` 的论证本身仍成立。
12. `design/task-group-api-and-options.md:276-280`（§3.3，SHOULD）—— "fail-fast 时组沿用失败成员自己的 outcome"
    → 改写为"有失败记录时（无论 token 是否已提交 FAIL_FAST）"。
13. `design/task-group-observability-and-verification.md:112`（必测矩阵 15，SHOULD）—— 该行仍写着"成员失败固定
    FAILED"（`FAILED` 已不是 `TaskOutcome` 值，属 0.2.0 重命名残留），需改为按 outcome 规则表述，并把
    lone failure/RUNNING 收敛列为显式用例。
14. `CHANGELOG.md:25`（SHOULD）—— 扩写该 breaking 条目，或在 0.2.0 下补一条 `### Fixes`。
15. 判定为**兼容、无需改动**：`docs/{zh,en}/migration-v0.2.md`（讲的是被删枚举到 MEMBER_CANCELED 的映射，
    非 RUNNING/失败场景）、`docs/{zh,en}/user-guide.md` 的成员级归因段落、`TaskGroupResult.java:62-66` javadoc、
    `TokenOutcomes.java:5-27`、`TaskFuture.java:44/62`、`adr/0001`、`adr/0002:219`、
    `design/AGENTS.md:11`、`wiki/`、`reports/*.html`、`demo/**`（只采样批量报告的字符串）、
    `blog`/`todo`/`verification`/`scripts`/`README*`/`findings.md`/`progress.md`/`task_plan.md`。

**附加观察（非缺陷，值得一句文档说明）**：`completionFuture()` 是 `Task.of(groupName, groupToken, completion)`
（`TaskGroup.java:88`），而 `completion` 按契约总是正常完成，所以
`group.completionFuture().outcome()` 恒为 `SUCCESS`，而 `group.completionFuture().get().outcome()` 可能是
`USER_FAILURE`。这是"完成 future 不承载组 outcome"这一既有设计的副作用，本次改动不影响，但容易被读成矛盾，
建议在 API 文档里点明。

**其他分支**：`feat/parallel-task-group-builder` 以旧包布局承载同样两处 MUST 修改
（`scope/ScopedTaskContractTest.java:95-99`、`scope/TaskGroupTest.java:363-367` 与 `scope/TaskGroup.java`
的 `deriveOutcome`）；若该分支也要修，需要同步移植。

---

## 附录 B：`DrainingBlockingQueue` 深挖结论——未发现缺陷

对 1600 行实现做了逐交错推演 + 经验验证（唤醒/级联矩阵 48 + 300 场景、与 JDK `LinkedBlockingQueue` 的
差分模糊 50 万次操作、20 万轮快照一致性检查），未发现挂起、丢失唤醒或状态机漏洞。已明确排除的区域：

1. **不存在"DRAINING 且 count==0"的不一致窗口**：每个递减路径都在同一个 `takeMonitor` 临界区内发布
   `DRAINED`（`poll` 803-811、`take` 895-896、`poll(t)` 973-975、`remove` 路径 1517-1518），消费者不会
   在"已无元素可发"的状态下无限等待。
2. **close 后无生产者漏入**：`open()`/lifecycle 在生产者监视器内复查（454/483/516/672/769）；`offer` 与
   `close` 竞争时要么在 DRAINING 之前落地、要么被拒，两种结果都不丢元素。
3. **锁序统一**（`putMonitor` → `takeMonitor`），所有信号 nudging 都在释放两把监视器之后发出
   （546-561/585-590/1084-1089），无死锁、无错锁信号。
4. **无丢失唤醒**：四种 guard 迁移（count 0→>0、capacity→<capacity、OPEN→DRAINING、→DRAINED）各有信号
   路径；Guava `Monitor` 在 `holdCount==1` 的 `leave()` 会重新评估 guard，被唤醒者先注销再离开。
5. **中断处理**：所有 `enterWhen(guard)` 都在离开监视器的 `try/finally` 之外（481/511/576/609），不会泄漏
   监视器、不会留下半修改链表、不会吞掉中断标志。
6. 批量删除的记账（`findPredecessor`/`ancestor`/`unlink`、`1L<<63` 位掩码）与 count↔链长守恒均通过差分验证。
7. 用户回调不在监视器内执行（契约 §12.6），含重入回调。
8. `close()` 426-428 的提前 `return` 跳过 nudging 是无害的（DRAINING 已由前一次 close 发布，且
   `enterWhen` 进入时会重新评估 guard）。

**非缺陷的次要问题**（可选清理，不建议与本次缺陷修复混在一个改动里）：

- `design/draining-queue-contract.md:121`（表格 `awaitDrained()` 行）声称 OPEN 状态"立即返回"，但代码会阻塞
  等待 DRAINED，且 `DrainingBlockingQueueTest:686` 正是断言 OPEN 下 1ms 超时返回 false——**文档错、代码与测试对**。
- `DrainingBlockingQueue.java:759` `addAll` 未做 `source == this` 守卫；JDK 对该情形的契约本就是 undefined
  （`AbstractQueue.addAll` 同样不守卫），不算偏离，最多补一个显式 IAE 更友好。
- `DrainingBlockingQueue.java:826-829` `remove(null)` 在 `requireMutationAllowed` 之前返回 false，因此
  DRAINED + THROW 策略下空探测不抛异常；可视为有意的"空探测不算变更"，但值得在契约里写明。
- 死代码：私有方法 `prepend`（1112）与 `unlinkLast`（1133）无调用点。
