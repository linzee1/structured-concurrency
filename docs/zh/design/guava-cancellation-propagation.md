# Guava ListenableFuture 取消传播机制

> 起因：`CancellationToken.bind` 曾从 addCallback 形态重写成链式调用（`withTimeout → transform → catchingAsync`），
> 目标是简洁、功能不变，实际引入两个 bug，6 个测试挂掉。两个 bug 都源于对 Guava 取消传播语义的隐含错误假设。
> 本文记录这套机制与正确用法。全部结论对 `guava-33.6.0-jre` 源码逐条验证过，每节附源码位置。

## 1. 事故复盘

改动前（正确）：

```java
FluentFuture<?> failFastFuture = FluentFuture.from(Futures.allAsList(futures))
        .catchingAsync(Throwable.class, ex -> Futures.immediateCancelledFuture(), directExecutor())
        .withTimeout(timeout, timer);

// 统一取消句柄：successfulAsList 要等全部 input 完成才终态，
// 所以在「某个成员已失败」的时刻它仍然 pending，取消它能级联到所有 inputs
ListenableFuture<?> allFutures =
        Futures.successfulAsList(Futures.successfulAsList(futures), submitCanceller);

failFastFuture.addCallback(new FutureCallback<>() {
    onSuccess: CAS SUCCESS;
    onFailure: CAS TIMEOUT/FAIL_FAST; allFutures.cancel(true);
}, directExecutor());
```

改动后（bug）：取消动作挪进 `catchingAsync` 的 fallback，统一取消句柄换成 `allAsList`。

| bug | 现象 | 根因 |
|---|---|---|
| 1 | 成员失败后 siblings 不被取消 | 成员失败的瞬间 `allAsList` 就已**失败终态**，对已完成 future 调 `cancel` 是 no-op，级联不到 inputs |
| 2 | 手动 `cancel()` 后 submitCanceller 不被取消 | 取消动作只写在 `catchingAsync` fallback 里，而 top-down 取消传播**绕过 fallback**（见 §2.1） |

## 2. 机制一：取消和失败是两条传播路径

失败沿链**向下游**（output 方向）传播：input 失败 → output 失败（transform 跳过、catching 进 fallback）。
取消沿链**向上游**（input 方向）传播：output 被取消 → `afterDone()` → `maybePropagateCancellationTo(input)`
→ input 被取消 → 一路传到链基。方向相反，是全部混乱的来源。

```text
output.cancel(true)                     member.future.cancel(true)
        │                                        │
        ▼ （向上游）                              ▼ （从链基向上）
catchingAsync → transform → withTimeout → allAsList → withTimeout → transform → catchingAsync
        │                                        │
        ▼ fallback 不跑                           ▼ fallback 会跑（见下）
```

### 2.1 catching 系：取消不进 fallback

`AbstractCatchingFuture.run()` 开头：

```java
if ((localInputFuture == null | ...) 
        || isCancelled()) {    // volatile read，检查的是输出(self)
    return;
}
```

top-down 取消先取消 catchingAsync 的**输出**，之后 input 完成触发 `run()`，`isCancelled()` 命中直接 return。
fallback 里的任何逻辑（包括取消 submitCanceller）在这条路径上是死代码。

### 2.2 bottom-up：取消会以 CancellationException 进 fallback

成员 future 被直接取消时，取消从链基向上传：input 取消 → `transform` 的 `run()` 对 cancelled input 执行
`cancel(false)`（`AbstractTransformFuture.run()`，源码注释 "inputFuture is cancelled... cancel(false)"）
→ 到达 catchingAsync 时 input 已 cancelled、输出未取消 → `getDone(input)` 抛 `CancellationException`
→ 被 `catch (Throwable t)` 接住（源码原注释 *"this includes CancellationException"*）
→ `exceptionType` 是 `Throwable.class` → 匹配 → **fallback 以 CancellationException 被调用**。

即同一个 fallback：top-down 不跑，bottom-up 跑。方向决定行为。

### 2.3 addCallback：取消也进 onFailure

`Futures.CallbackListener.run()` 对 future `getUninterruptibly`，cancelled future 抛
`CancellationException`（RuntimeException）→ `catch (RuntimeException | Error e)` → `onFailure(e)` 被调用。
**两个方向都进 onFailure**——这是 addCallback 与 catching 系的本质区别，也是旧实现正确的原因。

## 3. 机制二：组合 future 的取消级联（allAsList ≠ successfulAsList）

`AggregateFuture.afterDone()`：组合 future 被取消时遍历取消所有 inputs
（`future.cancel(wasInterrupted())`）。**前提：组合 future 还 pending 且 `futures` 字段未清空**。

| | `allAsList`（allMustSucceed=true） | `successfulAsList`（allMustSucceed=false） |
|---|---|---|
| 某 input 失败 | 组合 future **立即失败**（终态）→ 之后 cancel 它是 no-op，级联不到任何 input | 失败记 null，等**全部** input 完成才终态 → 此刻仍 pending |
| 某 input 被取消 | `processAllMustSucceedDoneFuture`: `futures = null; cancel(false)` → 组合 future 以 **cancelled** 终态完成，**不**级联其他 inputs | 同上保持 pending |
| 取消组合 future（pending 时） | 级联取消全部 inputs | 级联取消全部 inputs |
| 适用角色 | 「全部成功才算成功」的判定器 | **统一取消句柄** |

结论：**能安全当取消句柄用的，只有还 pending 的组合 future**。
`allAsList` 在第一个失败/取消时就终态，天然错过取消窗口——这是 bug 1 的机制。
`successfulAsList(futures, submitCanceller)` 嵌套一层，把任务 futures 和 submitter 拉进同一个可取消句柄。

## 4. setFuture 与 cancel 的双向桥

- **cancel-before-bind 生效的原因**：对已 cancelled 的 future 调 `setFuture(x)`，返回 false 且
  **取消 x**（`AbstractFuture.setFuture` 尾部 `localValue instanceof Cancellation` 分支）。
  所以 `futureToken` 先被 cancel、之后才 bind，链照样被取消。
- **二次 bind 不误伤**：对已**成功**的 future 调 `setFuture(x)` 只返回 false，x 不受影响。
- **cancel 向委托传播**：`futureToken.cancel` 对 `setFuture` 进来的 chain（DelegatingToFuture）
  会继续取消该 chain（`AbstractFuture.cancel` 的 Trusted 循环），并一路 `afterDone` 向上游传。
- **TimeoutFuture**：输出被取消 → 取消 delegate + 取消已调度的 timer task（防泄漏）。

## 5. 中断标志的传播

`cancel(mayInterruptIfRunning)` 把中断位存进 `Cancellation` 值，之后沿链用 `wasInterrupted()` 保持：

- `maybePropagateCancellationTo`: `related.cancel(wasInterrupted())` — 链式传播不丢中断位
- `AggregateFuture.afterDone` 级联 inputs 用 `wasInterrupted()`
- `allAsList` 对 cancelled input 的自我取消用 `cancel(false)`——那是「以取消完成」的语义，不是「要求取消」，不该带中断
- `cancel(false)` 契约：级联照常，但运行中任务不被中断（有测试钉死此行为）

## 6. 速查表

| API | input 被取消 | output 被取消 |
|---|---|---|
| `transform` | `run()` 里 `cancel(false)` 自己，向上传 | `afterDone` 向 input 传播 |
| `catching` / `catchingAsync` | 输出未取消：fallback 以 `CancellationException` 跑；输出已取消：早退 | 向 input 传播，fallback 不跑 |
| `withTimeout` | Fire → `cancel(false)` | 取消 delegate + timer task |
| `addCallback` | `onFailure(CancellationException)` | `onFailure(CancellationException)` |
| `allAsList` | 自身变 cancelled 终态，不级联 | pending 时级联全部；已 failed 时 no-op |
| `successfulAsList` | 保持 pending | 级联全部 |

## 7. 写给我们自己的规则

1. **取消也要触发处理逻辑 → `addCallback`**；只处理失败 → `catchingAsync`。fallback 里放取消动作前，先想清楚 top-down 取消怎么到达它（答案：到不了）。
2. **统一取消句柄选还 pending 的组合 future（`successfulAsList`）**，不要选会提前终态的（`allAsList`）。
3. **归因不靠猜**：`isCancelled()` 事后看不出谁取消的。token 状态机先 CAS 再执行取消动作，观察者读状态即可归因（`CancellationToken.transitionTo` 的 CAS-notify-cancel 顺序）。
4. 链式简洁是有代价的：每层组合器对取消的处理不同，重写前先核对 §6 的表格。

## 源码索引（guava-33.6.0-jre）

| 结论 | 位置 |
|---|---|
| 取消向 input 传播 | `AbstractFuture#maybePropagateCancellationTo` |
| setFuture 对已取消 future 取消入参 | `AbstractFuture#setFuture` |
| cancel 向 DelegatingToFuture 传播 | `AbstractFuture#cancel` |
| catching 的 isCancelled 早退 / catch Throwable 含取消 | `AbstractCatchingFuture#run` |
| transform 对 cancelled input 的 cancel(false) | `AbstractTransformFuture#run` |
| 超时输出取消时清 timer | `TimeoutFuture#afterDone`、`TimeoutFuture.Fire#run` |
| 组合 future 取消级联 | `AggregateFuture#afterDone` |
| allAsList 对 cancelled input 的自我取消 | `AggregateFuture#processAllMustSucceedDoneFuture` |
| 取消也进 onFailure | `Futures.CallbackListener#run` |
