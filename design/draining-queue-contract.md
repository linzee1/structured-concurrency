# DrainingBlockingQueue 逐渐关闭契约设计

> 逐渐关闭(draining close)有界 FIFO 阻塞队列的契约规范。
> 与 `ClosableBlockingQueueV2` 的"突然关闭"模型相对：关闭**不丢弃、不隔离**已入队元素，
> 而是让消费端把存量排干之后才对外呈现"已终结"。
> 本文档是独立契约，不修改 `closable-blocking-queue-shutdown-contract.md`；两者语义互斥，实现不得混用。

## 0. 与突然关闭契约的关系

| 维度 | 突然关闭(V2，已有文档) | 逐渐关闭(本文档) |
|---|---|---|
| 关闭时已入队元素 | 立即隔离进 recovery，普通取出通道不再返回 | 继续可被正常取出，直到排干 |
| 关闭后 `take()` | 立即抛 NSE / 返回 poison | 有存量时返回真实元素；排干后才 poison/NSE |
| 关闭后 `size()` | 0(只计"活"队列) | 真实剩余元素数，随消费递减至 0 |
| 恢复通道 | `remainingList()`/`drainTo` 是一等公民 | **不存在**——不需要恢复，因为没有隔离 |
| 适用场景 | 紧急停止，宁可积压也要立即释放调用方 | 优雅停止，不允许丢任何已接收任务 |

一个队列实例只能实现其中一种契约。判据：关闭后 `poll()` 在仍有存量时返回真实元素的是逐渐关闭，返回 null/poison 的是突然关闭。

## 1. 核心心智模型

生命周期为三态单向机：`OPEN → DRAINING → DRAINED`。

```
OPEN      : 正常 BlockingQueue。生产者放入，消费者取出。
DRAINING   : close() 已调用。生产端永久拒写；消费端继续取出存量，直到队列排干。
DRAINED   : DRAINING 且队列已空。终态，不可回退。
            此后所有取出通道返回"终结信号"(poison 或 null/异常)，由配置决定。
```

关键不变量：**从 OPEN 到 DRAINED，任何已经成功 `offer`/`put`/`add` 返回 `true` 的元素，最终一定被某个消费者取走(或被 `drainTo` 排空)。** 逐渐关闭契约下不存在"调用成功但元素消失"的状态。

"排干"的线性化点：若 `close()` 线性化时队列为空，`close()` 本身必须同时完成 `DRAINING → DRAINED`；否则，某个取出或排空操作把 DRAINING 状态下最后一个真实元素取走的那一刻，在该操作的临界区内迁移到 DRAINED。该迁移与元素所有权转移原子一致——拿到最后元素的调用返回时，`isDrained()` 已为 true。

## 2. 方法分组

分组边界与突然关闭契约一致(便于两份文档对照)，但各组在 DRAINING/DRAINED 下的行为不同。

| 分组 | 方法 | OPEN | DRAINING(有存量) | DRAINED |
|---|---|---|---|---|
| `SPECIAL_VALUE` | `offer(e)` | 满 false | `false` | `false` |
| | `poll()`/`peek()` | 空 null | 真实元素 | poison；无 poison 时 `null` |
| `THROWS` | `add(e)` | 满抛 ISE | `IllegalStateException` | 同左 |
| | `remove()`/`element()` | 空抛 NSE | 真实元素 | poison；无 poison 时 `NoSuchElementException` |
| `BLOCKING` | `put(e)` | 满则阻塞 | `IllegalStateException`(closed 子类)，**不阻塞** | 同左 |
| | `take()` | 空则阻塞 | 有存量立即返回；无存量阻塞至 DRAINED(见 §4.2) | poison 或 NSE，**不阻塞** |
| `TIMED_BLOCKING` | `offer(e,t,u)` | 阻塞至成功/超时 | `false`，**不等待** | `false` |
| | `poll(t,u)` | 阻塞至有元素/超时 | 有存量立即返回；无存量等待 min(剩余timeout, 到达DRAINED) | poison 或 `null`，**不等待** |
| `MUTATION` | `clear`/`remove(Object)`/`removeIf`/`removeAll`/`retainAll` | 修改内容 | **照常执行**——DRAINING 下允许清理存量(见 §4.3) | NOOP 或 THROW，由 `mutations` 配置 |
| `QUERY` | `size`/`isEmpty`/`remainingCapacity`/`contains` | 反映真实状态 | 反映真实存量；`remainingCapacity` 返回 0(生产端已关) | `size` 0 / `isEmpty` true |
| `RECOVERY` | `drainTo` | 排空当前元素 | 排空当前存量 | 返回 0 |
| `ITERATOR` | `iterator`/`spliterator`/`stream` | 弱一致、late-binding 遍历(LBQ 风格，`Iterator.remove()` 作用于队列) | 弱一致观察剩余存量 | 空 |
| `QUERY` | `toArray`/`toString` | FIFO 引用快照 | FIFO 剩余元素引用快照 | 空快照 |

`offer(timeout)`/`poll(timeout)` 虽返回 `false`/`null`，仍属 `TIMED_BLOCKING`。`remove()`(无参)属 `THROWS`，`remove(Object)` 属 `MUTATION`。与突然关闭契约的分组划分完全相同。

`spliterator()` 以及由它创建的 `stream()`/`parallelStream()` 为弱一致、late-binding：首次遍历前后并发入队或出队的元素可以被观察到、跳过或只观察一次，不承诺精确快照，也绝不把 poison 当作元素交付。`toArray()` 则在持锁时复制一次 FIFO 元素引用；`toString()` 先取得同样的引用快照，再在锁外调用各元素的 `toString()`。

## 3. 设计原则

1. **不丢元素**：这是逐渐关闭存在的唯一理由。任何规则若导致已成功写入的元素不可达，该规则作废。
2. **生产端单向关门，消费端自然排空**：close() 只关生产端，消费端行为由"还剩多少存量"驱动，而不是由"是否已 close"驱动。
3. **DRAINED 之前的取出通道不返回 poison**：poison 是"终结信号"，存量未清时返回 poison 会被消费者误判为结束，违反原则 1。
4. **不新增配置维度**：仍只有 `poison` 和 `mutations` 两个维度，与突然关闭契约的配置模型完全同构。
5. **查询通道永远诚实**：`size()`/`isEmpty()` 在所有状态下都反映真实存量。不存在突然关闭契约里"空但有数据"的悖论，因此也不需要 `remainingList()`。

## 4. 规则优先级

**核心机制不变：优先级瀑布，首条匹配 rule 独占。** 但逐渐关闭的瀑布层级与突然关闭**不同**——这是两份契约最关键的结构差异。

| 优先级 | Rule | 适用条件 | 行为 |
|---|---|---|---|
| 前置校验 | poison 身份校验 | 所有写入方法的参数 | 与 poison `equals` 相等时抛 `IllegalArgumentException`(见 §6.2) |
| 1(最高) | 存量优先 | DRAINING 且队列非空时的**取出**方法 | 返回真实元素，poison 不介入 |
| 2 | CLOSED 生产拒写 | DRAINING/DRAINED 的**写入**方法 | `offer`/`offer(t)` 返回 false；`put`/`add`/`addAll` 抛 ISE(closed 子类) |
| 3 | DRAINED 终结信号 | DRAINED 的取出方法 | poison 策略返回 poison；否则按方法族返回 null 或抛 NSE(closed 子类) |
| 4 | `TIMED_BLOCKING` | 带计时阻塞方法在 OPEN/DRAINING 空队列时 | 等待至成功、超时或到达 DRAINED，先满足者生效 |
| 5 | `MUTATION` | clear/remove(Object)/removeIf/removeAll/retainAll | OPEN/DRAINING 照常执行；DRAINED 起 NOOP 或 THROW |
| 6 | `QUERY`/`RECOVERY`/`ITERATOR` | 查询、排空、迭代 | 按 §2 分组契约执行 |

### 4.1 与突然关闭瀑布的对照

| 差异点 | 突然关闭 | 逐渐关闭 |
|---|---|---|
| 最高优先级 | POISON 策略(关闭后立即返回 poison) | **存量优先**(关闭后先返回真实元素) |
| poison 生效时机 | close 线性化点之后立即 | DRAINED 线性化点之后才生效 |
| 查询通道 | 关闭后归零(只计"活"队列) | 全生命周期反映真实存量 |
| 恢复通道优先级 | 独立分组，始终可用 | 不需要，`drainTo` 退化为普通排空 |

### 4.2 `take()` 在 DRAINING 空队列下的阻塞语义

DRAINING 且仍有存量时，`take()` 应立即返回真实元素；不应返回 poison，也不应继续等待。`close()` 线性化时若队列已经为空，必须在同一临界区直接发布 `DRAINED`，因此不会产生"关闭后空队列永久等待"的状态。

关闭前已经阻塞的 `take()` 可能与 `close()` 并发：如果 `close()` 发现队列为空并发布 DRAINED，它必须唤醒这些调用，使其按 DRAINED 规则返回 poison 或抛 NSE。若最后一个元素由另一个消费者取走，则该消费者负责在同一临界区发布 DRAINED 并唤醒等待者。

阻塞调用的退出条件按优先级：

1. **线程中断**：优先抛 `InterruptedException`；不得被关闭结果吞掉。
2. **真实元素可取**：返回一个存量元素。
3. **DRAINED 到达**：返回 poison 或抛 NSE。

新元素到达不是 DRAINING 下的退出条件，因为生产端已经关闭。实现必须保证 `close()` 和最后一次排干操作唤醒所有阻塞的 `take()`/`poll(t)`/`put`/`offer(t)`，并且 `put` 不会在关闭后继续等待。

### 4.3 MUTATION 在 DRAINING 下的特殊地位

这是与突然关闭契约的又一个实质差异：DRAINING 下 `clear()`/`removeIf` 等**照常执行**。

理由：DRAINING 的语义是"排干存量"，而 mutation 方法本身就是排空存量的合法手段之一。禁止它们等于剥夺了用户在关闭阶段主动清理队列的能力。DRAINED 之后存量已为零，mutation 退化为 NOOP(默认)或 THROW(可配)，此时禁止才有意义——防止用户误以为还在操作真实数据。

`drainTo` 在所有状态下都可用且语义不变：排空当前真实元素。DRAINING 下它等价于"立即排干存量"，是消费者之外的第二条排空通道。这与突然关闭契约一致，但动机不同：那里是恢复隔离元素，这里只是普通排空。

## 5. 状态判定与并发不变量

### 5.1 状态查询

| 方法 | OPEN | DRAINING | DRAINED |
|---|---|---|---|
| `isShutdown()` | false | true | true |
| `isDraining()` | false | true | false |
| `isDrained()` | false | false(即使瞬间为空) | true |
| `awaitDrained()` | 立即返回 | 阻塞等待 DRAINED | 立即返回 |
| `awaitDrained(timeout, unit)` | 立即返回 true | 等待 DRAINED 或超时 | 立即返回 true |

`isShutdown()`、`isDraining()` 与 `isDrained()` 分离是必要的：`isShutdown()` 告诉生产者"别再写了"，`isDraining()` 表示存量仍在排空，`isDrained()` 告诉消费者"真的结束了"。合并其中任意两个都会让调用方难以区分"暂时空"和"永久结束"。

`awaitDrained()` 只等待 `DRAINED` 状态到达，不承诺等待已准入阻塞调用全部退出；后者属于实现可增强的诊断能力，不属于主契约。`awaitDrained(timeout, unit)` 在状态到达时返回 `true`，超时返回 `false`；两者都可响应线程中断并抛出 `InterruptedException`。

### 5.2 并发不变量

| 不变量 | 说明 |
|---|---|
| 幂等 | 多次 `close()` 等价于首次调用，无重复迁移、重复唤醒或重复回调副作用 |
| 单向 | `OPEN → DRAINING → DRAINED`，无回退；关闭后不得重新开放生产端 |
| 关闭线性化 | `close()` 在一个临界区内关闭生产准入，并与正在提交的 `put`/`offer` 排定先后；先完成关闭的调用不得再提交元素 |
| 关闭后无生产阻塞 | `close()` 返回后，新的 `put`/`offer(timeout)` 不得等待容量；前者抛关闭异常，后者返回 `false` |
| 阻塞调用释放 | `close()` 必须唤醒已经阻塞的 `put`、`offer(timeout)`、`take`、`poll(timeout)`；被唤醒后必须重新检查规则，不得继续等待或提交非法写入 |
| 排干线性化 | DRAINING 下最后一个真实元素被取走或通过 `drainTo` 排空时，在同一临界区完成 `DRAINING → DRAINED` 迁移 |
| DRAINED 发布 | DRAINED 状态发布与最后一个元素的所有权转移有明确先后；后续取出调用不得再等待，也不得返回真实元素 |
| happens-before | `close()` 线性化前已入队元素对后续取出、迭代和 `drainTo` 可见；DRAINED 状态发布对 `awaitDrained()` 及后续取出调用可见 |
| 中断优先 | 阻塞方法观察到外部线程中断时优先抛出 `InterruptedException`；关闭结果不得吞掉中断。若实现选择优先返回关闭结果，必须先恢复线程中断标志 |
| 排干等待 | `awaitDrained()` 只等待 `DRAINED` 状态到达；超时版本在状态到达时返回 `true`，超时返回 `false` |
| 用户回调隔离 | `drainTo` 的目标集合、谓词和其他用户代码不得在队列生命周期锁/队列锁内执行 |
| 锁顺序 | 多锁实现统一按 `lifecycleLock → queueLock` 获取，禁止反向；阻塞等待不得持有 `lifecycleLock` |

## 6. 配置模型

与突然关闭契约完全同构的两个维度，语义因状态机不同而重新锚定。

### 6.1 `poison`

关闭后**且 DRAINED 之后**，值返回型取出方法返回该对象：`poll`、`poll(timeout)`、`peek`、`take`、`element`、`remove()`。未配置时，Special-value 方法返回 `null`，Throws/Blocks 方法抛 `NoSuchElementException`(closed 子类)。

与突然关闭契约的差异：poison 的生效时机从"close 之后"推迟到"DRAINED 之后"。DRAINING 有存量时 poison 绝不出现——这是优先级 1(存量优先)的直接推论。

### 6.2 poison 身份校验

所有写入路径(含 `addAll` 预校验)拒绝与 poison **equals 相等**的元素，抛 `IllegalArgumentException`。

注意这是与 V2 实现的有意分歧：V2 按引用(`==`)拒绝，导致字符串/intern 场景行为不可预测。逐渐关闭契约采用 equals 拒绝，牺牲"equals 相等但非同一引用也能写入"的理论灵活性，换取行为确定性。消费端仍以 `==` 判定 poison——写入侧 equals 拒绝 + 消费侧 `==` 判定的不对称组合是唯一自洽方案：写入侧宁可错杀，消费侧宁可错放。

### 6.3 `mutations`

`NOOP`(默认)或 `THROW`，控制 **DRAINED 之后**的 `clear`、`remove(Object)`、`removeIf`、`removeAll`、`retainAll`。DRAINING 下这些方法照常执行，不受该配置管辖(见 §4.3)。

### 6.4 预设

```java
DrainingBlockingQueue.ShutdownPolicy.empty()       // 无 poison + NOOP mutations
DrainingBlockingQueue.ShutdownPolicy.poison(p)     // poison + NOOP
DrainingBlockingQueue.ShutdownPolicy.throwing()    // 无 poison + THROW

DrainingBlockingQueue.ShutdownPolicy.builder()
    .poison(p)
    .mutations(DrainingBlockingQueue.MutationsStrategy.THROW)
    .build();
```

预设组合自洽；覆盖只改单维度。非法组合(null poison 用于 poison 预设等)构造期拒绝。

## 7. 关闭后完整契约表

按默认预设 `empty()` 展开(写入固定 + 取出无 poison + 变异 NOOP)，队列为空且已 DRAINED：

| 方法 | DRAINED 后行为 |
|---|---|
| `offer(e)` / `offer(e,t,u)` | `false` |
| `put(e)` | `IllegalStateException`(closed 子类) |
| `add(e)` / `addAll` | `IllegalStateException` |
| `poll()` | `null` |
| `poll(t,u)` | `null`，不等待 |
| `take()` | `NoSuchElementException`(closed 子类)，不阻塞 |
| `peek()` | `null` |
| `element()` | `NoSuchElementException`(closed 子类) |
| `remove()` | `NoSuchElementException` |
| `remove(Object)` | `false`(NOOP) |
| `clear()` | no-op |
| `removeIf` / `removeAll` / `retainAll` | `false` |
| `size()` / `isEmpty()` | `0` / `true` |
| `remainingCapacity()` | `0` |
| `iterator()` / `toArray` / `stream` | 空 |
| `contains` | `false` |
| `drainTo` | 返回 0 |
| `isShutdown()` / `isDraining()` / `isDrained()` | `true` / `false` / `true` |
| `awaitDrained()` / `awaitDrained(timeout, unit)` | 立即返回 / 立即返回 `true` |

`poison(p)` 预设将 DRAINED 后所有值返回型取出结果改为 `p`。

## 8. 等待 API 与 Service 边界

`awaitDrained()` 和 `awaitDrained(long timeout, TimeUnit unit)` 属于队列生命周期 API，但它们只观察 `DRAINED` 状态：

- 无限等待版本在 DRAINED 到达后返回，响应线程中断并抛出 `InterruptedException`
- 超时版本在 DRAINED 到达时返回 `true`，超时返回 `false`
- 两者都不承诺等待已准入阻塞调用全部退出，也不表示后台资源已经停止

Guava `Service` 不进入主契约。Draining 模型的关键事件是“关闭生产端”和“存量排干”，不是传统 Service 的“启动后停止”；若需要 `ServiceManager` 集成，应提供独立适配层，将 `close()` 映射为停止请求、将 `isDrained()` 映射为 terminated。

## 9. 异常类型

本契约不引入自定义异常类型，关闭行为复用 JDK 方法族的既有异常：

- 生产端被关闭时（`put`/`add`/`addAll` 等）抛 `IllegalStateException`，消息以 "queue is closed" 开头
- 消费端在 DRAINED 且未配置 poison 时（`take`/`remove`/`element` 等）抛 `NoSuchElementException`，消息以 "queue is drained" 开头

两者都是 `RuntimeException` 子类，不改变 `BlockingQueue` 方法签名。调用方按各自方法族的 JDK 异常约定 catch 即可，无需感知生命周期专用类型：写关闭意味着"重试无用"，读关闭(无 poison)意味着"队列已空且永不再有"。

## 10. 调用约定

- **优雅关闭生产-消费循环**：`close()` 后消费者继续 `take()`，直到拿到 poison(或捕获 `NoSuchElementException`)退出。不需要 `remainingList()`，不存在隐藏存量。
- **判定关闭与暂时空队列**：`poll()` 返回 null 时，若需区分"DRAINING 暂时空"和"DRAINED 永久空"，调用 `isDrained()`。`isShutdown()` 只说明生产端关了，不说明存量排干。
- **size/isEmpty 全程可信**：任何状态下都反映真实存量，`while (!q.isEmpty()) process(q.poll())` 在所有生命周期阶段都是安全的(尽管并发下 size 只是弱一致)。
- **中断语义**：阻塞方法观察到外部中断时优先抛 `InterruptedException`；关闭结果不得吞掉中断，上层取消框架可依赖中断标志。
- **等待排干**：用 `awaitDrained()` 等待 DRAINED；带超时版本用于有界等待。不要把它理解成等待所有调用线程退出。
- **DRAINING 下的主动清理**：`clear()`/`removeIf`/`drainTo` 可用，用于"放弃剩余存量、立即到达 DRAINED"的场景。清空最后元素的操作必须在同一临界区触发 DRAINED 迁移。

## 11. 与突然关闭契约的迁移指南

若用户从 V2 迁移到逐渐关闭实现：

| V2 用法 | 逐渐关闭等价物 | 注意事项 |
|---|---|---|
| `close()` 后调 `remainingList()` 找回剩余 | 不需要——继续 `take()` 直到 poison/NSE | 删除 remainingList 调用，改为消费循环 |
| `close()` 后 `size()==0` 判断"已关" | `isDrained()` | `isShutdown()` 不等于排干 |
| `catch (NoSuchElementException)` 判断关闭 | 直接 `catch (NoSuchElementException)`(消息含 "queue is drained") | 无需迁移 |
| `catch (IllegalStateException)` 判断关闭 | 直接 `catch (IllegalStateException)`(消息含 "queue is closed") | 无需迁移 |
| 依赖 `Service` 状态机 | `isShutdown()`/`isDrained()` + 可选 listener | Service 集成是独立适配层，不在本契约 |

## 12. 实现参考：借鉴当前 `ClosableBlockingQueue` 的关闭技巧

以下机制不是对外语义的一部分，但推荐实现直接借鉴，以满足 §5.2 的关闭/唤醒/竞态要求。

### 12.1 等待谓词内嵌生命周期状态

阻塞等待不要只判断"有元素/有容量"，而应把生命周期状态直接编入 Guard 或 Condition 的继续条件：

- 生产者等待条件：`state == OPEN && size < capacity`
- 消费者等待条件：`state == OPEN && size > 0`，DRAINING 时若有存量应立即满足，若无存量则等待 DRAINED
- 效果：`close()` 一线性化，等待条件立即不可能继续用于无限等待；实现不会把"关闭后重新等待"留给竞态

### 12.2 关闭后主动触发条件重评估

关闭完成后，不要依赖"下次自然唤醒"。应在同一生命周期步骤内主动让所有等待者重新评估条件：

- 单锁 `Condition` 实现：`notEmpty.signalAll()` + `notFull.signalAll()`
- Guava `Monitor` 实现：进入并退出相关 Monitor，促使 Guard 重估并放行已满足的等待者

这能保证阻塞中的 `put`/`offer(t)`/`take`/`poll(t)` 在 `close()` 返回后快速退出，而不是滞留到超时。

### 12.3 进入前准入、临界区内再检查、最后才提交

阻塞方法建议统一为三段式：

1. **准入**：开始阻塞调用前，先检查生命周期；若已关闭，按方法族直接返回关闭结果
2. **再检查**：获得队列锁/Monitor 后，在提交前再次确认状态；因为等待期间可能发生了关闭
3. **提交**：最后一步才执行 `enqueue`/`dequeue` 或状态迁移，确保元素转移与状态判断原子一致

这对应现有实现里的 `beginBlockingCall` → `enterWhen` → 生命周期复查 → `enqueue/dequeue`，可以封闭"先通过等待谓词、后被关闭截断"的竞态。

### 12.4 监控边界

`awaitDrained()` 的主契约只等待 `DRAINED` 状态到达，不要求等待阻塞调用退出。队列不维护也不暴露历史活跃调用计数；调用方若需要调用耗时、并发数或等待量等指标，应在自身的调用包装层采集。

### 12.5 关闭不使用线程中断

关闭协议应通过谓词重估、锁内状态发布和主动唤醒释放调用方，而不是调用 `Thread.interrupt()` 打断用户线程。中断属于外部取消信号，不应被队列内部生命周期征用。

### 12.6 用户代码移出临界区

`drainTo` 的目标集合、`removeIf` 谓词、`removeAll`/`retainAll` 的 `contains`、`remove(Object)` 的 `equals`、`addAll` 的 poison 校验，以及 `toString`/`forEach` 的用户回调，都不得在持有队列 Monitor 时执行。推荐模式是：

1. 先在临界区内把要交付的元素摘下
2. 出锁后再调用用户集合或谓词

这能避免用户回调拖慢关闭、阻塞唤醒，或在回调里重入队列造成死锁。

### 12.7 批量删除与观察的一致性边界

`removeIf`、`removeAll` 和 `retainAll` 每批最多处理 64 个节点：锁内抓取节点引用，锁外执行谓词/`contains`，再锁内确认节点仍在队列后摘除。这样避免为整条队列创建快照，也避免用户代码占用队列 Monitor。

这三个操作不是全调用原子事务：前一批已经摘除后，后一批用户代码抛异常，前一批不会回滚；尚未进入摘除阶段的当前批不会删除。并发消费者可能在重锁前取走候选节点，此时该节点被跳过；并发生产者在遍历期间加入的节点也可能被观察到或跳过。`remove(Object)` 同样是“锁内取候选、锁外 equals、锁内按身份确认”的两阶段操作；候选被并发取走时会继续寻找下一个匹配项。

若两阶段操作之间队列到达 `DRAINED`，`mutations=THROW` 可以使重锁阶段抛出 `IllegalStateException`，即使较早快照中存在候选元素；`mutations=NOOP` 则不再摘除节点。这是终态 mutation 策略在线性化边界上的正常结果，调用方不应把先前快照视作删除承诺。
