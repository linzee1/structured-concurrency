# E3. 排干式关闭的 BlockingQueue：DrainingBlockingQueue 的设计目标、约束与契约

> 一个把"优雅关闭"做成队列一等公民的有界 FIFO 阻塞队列：close() 永久拒绝新生产，消费端继续排干存量直到终态，排空后消费端得到终结信号（毒丸或异常）。基于 Guava Monitor 双管程 + 单向三态生命周期 OPEN → DRAINING → DRAINED。

## 设计目标

JDK BlockingQueue 没有关闭语义。任务队列排空后 `take()` 永久阻塞，放满后 `put()` 永久阻塞。传统解法是毒丸（poison pill），但毒丸只解决消费端、需要业务代码识别标记、多消费者还容易漏分。

`DrainingBlockingQueue` 的设计目标，是把"优雅关闭"做成队列支持的能力：

1. **不丢元素**。关闭只关生产端，关闭前已成功入队的元素必须继续可达。这是排干式关闭存在的唯一理由。
2. **关闭 ≠ 中断**。关闭不 `interrupt()` 任何线程。等待者通过 guard 重估自行醒来，以明确结果退出——写端拒绝、消费端继续。
3. **生产端单向关门，消费端自然排干**。`close()` 后 `put` / `offer(t)` / `add` 立即拒绝；`take` / `poll(t)` 有存量立即返回真实元素，直到排空。
4. **排空线性化点明确**。取走最后一个元素的调用，在其临界区内同时发布终态；调用返回时 `isDrained()` 已为 true。
5. **查询通道诚实**。`size()` / `isEmpty()` 全程反映真实存量；`isShutdown()` 与 `isDrained()` 分开表达"生产端已关"与"已排空"。
6. **关闭不被用户代码阻塞**。用户回调（drain 目标集合的 add、元素 equals、removeIf 谓词）都在 monitor 之外执行，再慢也拖不住关闭。
7. **关闭幂等、可等待**。并发 `close()` 只生效一次；`awaitDrained()` 阻塞到终态。

## 约束

实现前立下的边界：

1. **有界 FIFO，元素非 null**，行为对齐 `LinkedBlockingQueue`。
2. **三态单向机**。`OPEN → DRAINING → DRAINED`，不可回退；DRAINED 是唯一终态。
3. **双管程协同**。`putMonitor` 保护入队与容量，`takeMonitor` 保护出队与空，`AtomicInteger count` 串联两端。
4. **关闭判定折叠进 guard 谓词**。生命周期状态是谓词组成项，等待者自己"看见"关闭，无需外部唤醒信号。
5. **不引入自定义异常**。写拒绝抛 `IllegalStateException`，排空后的读终结抛 `NoSuchElementException`，调用方按方法族既有约定 catch。
6. **用户代码不进锁**。`equals` / 谓词 / 集合回调在 monitor 外执行，持锁期间绝不调用业务代码。
7. **无需恢复通道**。元素从未被隔离，不需要 `remainingList()` / recovery。

## 新契约

| 时机 | 行为 |
|---|---|
| 正常操作 | 标准 BlockingQueue 语义：FIFO、有界、put/take 双管程并发 |
| `close()` 后生产（put / offer(t) / add / addAll ...） | 拒绝，抛 `IllegalStateException`（消息含 "queue is closed"）或返回 `false`，不等待 |
| DRAINING 消费（take / poll / remove / element） | 有存量立即返回真实元素 |
| DRAINED 消费，配置了毒丸 | 返回毒丸对象（仅终结信号，不是元素） |
| DRAINED 消费，未配置毒丸 | 抛 `NoSuchElementException`（消息含 "queue is drained"）；`poll` / `peek` 返回 `null` |
| DRAINING 集合变更（clear / remove / removeIf ...） | 照常执行，可用来主动放弃存量 |
| DRAINED 集合变更 | 按 `DrainingBlockingQueue.ShutdownPolicy.mutations` 策略：NOOP 返回 false，或抛 `IllegalStateException` |
| `drainTo` | 任何状态下可用；排空后返回 0 |
| `iterator()` | 弱一致 live 遍历，`remove()` 作用于队列，遵循同一变更策略 |
| 外部 `interrupt()` 等待中 | 抛 `InterruptedException`（与关闭正交，原样传播） |
| `awaitDrained()` | 阻塞直到 DRAINED（关闭且排空） |

三个关键决策：

**为什么是排干而不是隔离恢复？** 关闭时已入队元素继续可被正常取出，直到排空。相比"突然关闭 + recovery list"模型，没有 `remainingList()`、没有恢复所有权转移，语义更简单：任何成功 `offer` / `put` / `add` 返回 true 的元素，最终一定被某个消费者取走。

**为什么用 JDK 异常而不是自定义异常？** `IllegalStateException` 与 `NoSuchElementException` 都是 `RuntimeException`，不改变 `BlockingQueue` 方法签名。调用方按各自方法族的 JDK 约定 catch 即可，无需感知生命周期专用类型。

**为什么 DRAINING 时消费端还返回真实元素？** 原则：关闭前已成功写入的元素不可丢弃。若存量未清就返回 poison，消费者会误判为结束，违反"不丢元素"。

## 现有实现支持功能

- **队列核心**：四种阻塞方法（put / take / offer(t) / poll(t)）、非阻塞 offer / poll / peek、`drainTo`（锁内摘除、锁外回调）、`clear`、`remove`、`removeIf` / `removeAll` / `retainAll`、弱一致迭代器（Java 8 LinkedBlockingQueue 形状，支持基于身份移除）。
- **生命周期**：`close()`（幂等）、`isShutdown()` / `isDraining()` / `isDrained()`、`awaitDrained()`（带超时版本）。
- **关闭语义**：`DrainingBlockingQueue.ShutdownPolicy` 两个维度——毒丸（poison）与排空后的集合变更策略（NOOP / THROW）。
- **双管程并行**：put 与 take 各自独立 Monitor，生产和消费并发不互斥。

## 技术设计

### 双管程布局与生命周期 guard 谓词

与 `LinkedBlockingQueue` 双锁一致，`putMonitor` 保护 tail 与容量等待，`takeMonitor` 保护 head 与元素等待，`AtomicInteger count` 串联两端。生命周期状态直接编入谓词：

```java
private final Monitor.Guard takeReady = new Monitor.Guard(takeMonitor) {
    public boolean isSatisfied() {
        return count.get() > 0 || lifecycle == Lifecycle.DRAINED;
    }
};
private final Monitor.Guard putReady = new Monitor.Guard(putMonitor) {
    public boolean isSatisfied() {
        return count.get() < capacity || lifecycle != Lifecycle.OPEN;
    }
};
```

谓词两项分别对应两类唤醒：有数据/有容量（正常流转），或生命周期状态变化（关闭取代流转）。关闭状态折叠进谓词——等待者不需要被显式通知，关闭后 guard 一满足，`enterWhen` 返回，方法体自行决定结果是入队/出队还是拒绝。消费端在 DRAINING 无存量时继续等待到 DRAINED，而不是被关闭"踢出"。

### 关闭即状态迁移：close()

关闭在双锁内原子完成，随后主动让两侧 guard 重估：

```java
public void close() {
    fullyLock();                            // 固定锁序 putMonitor → takeMonitor
    try {
        closeAdmission();                   // 双锁内关闭后续阻塞调用
        if (lifecycle != Lifecycle.OPEN) {
            return;                         // 幂等
        }
        lifecycle = Lifecycle.DRAINING;
        publishDrainedIfEmptyLocked();      // 若已空，立即 DRAINED
    } finally {
        fullyUnlock();                      // leave 触发两侧 guard 重估
    }
    signalTakeReady();
    signalPutReady();
}
```

### 排干线性化点

`publishDrainedIfEmptyLocked()` 在"取走最后一个元素"的临界区内执行：任何把 DRAINING 状态下最后一个元素取走或清掉的操作（take / poll / remove / removeIf / clear / drainTo），都必须在同一临界区把 `DRAINING` 迁移到 `DRAINED`。拿到最后元素的调用返回时，`isDrained()` 已为 true。

### 锁外回调：关闭不被用户代码阻塞

凡是可能慢的用户代码都移出 monitor：

- `drainTo`：锁内只做摘除（所有权的线性化点），锁外逐个 `target.add`。
- `remove(Object)` / `removeIf`：锁内快照节点 + item，锁外跑 `equals` / 谓词，再按身份重锁校验后摘除。
- 迭代器：next() 锁内推进，remove() 锁内按身份摘除，用户 equals 不在持锁期间调用。

### 弱一致迭代器

Java 8 `LinkedBlockingQueue.Itr` 形状：构造 O(1) 捕获首节点，`currentElement` 缓存避免持锁期间访问用户对象，`nextNode` 从自链已出队节点恢复，`remove()` 按身份摘除并遵循关闭后的变更策略。

## 示例

完整可运行测试见 `src/test/java/io/github/huatalk/parallelinscope/queue/DrainingBlockingQueueTest.java`。

```java
Integer poison = Integer.valueOf(-1);
DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2, poison);
queue.put(1);
queue.put(2);

queue.close();          // 生产端关闭；消费端继续排干
System.out.println(queue.isDraining());   // true
System.out.println(queue.take());         // 1（真实元素）
System.out.println(queue.take());         // 2（真实元素）
System.out.println(queue.isDrained());    // true，最后一个元素取走时已发布
System.out.println(queue.take());         // -1（毒丸，终结信号）

try {
    queue.put(3);                         // 生产端已关
} catch (IllegalStateException shutdown) {
    System.out.println("post-close put -> " + shutdown.getMessage());
}
```

输出：

```
true
1
2
true
-1
post-close put -> queue is closed: put
```

关闭前已入队的 `[1, 2]` 被正常取走，毒丸只在排空后出现；关闭后的 `put` 抛 `IllegalStateException` 且不阻塞。

## 测试验证

`DrainingBlockingQueueTest` 覆盖 47 个用例，与本设计直接相关的关键项：

- `closeEmptyQueuePublishesDrainedImmediately`：空队列关闭立即达终态
- `blockedConsumersReceiveStoredElementsThenTerminalSignal`：关闭后阻塞消费者先取真实元素再收终结信号
- `poisonOnlyAppearsAfterTheLastRealElement`：毒丸只在最后一个真实元素之后出现
- `producersAreRejectedAfterClose`：关闭后生产端全部拒绝
- `iteratorRemoveRemovesFromLiveQueue`：迭代器 remove 作用于真实队列
- `removeIfDoesNotHoldTheLockWhileEvaluatingThePredicate`：谓词在锁外执行，不阻塞并发操作
- `mutationsDrainNormallyWhileDrainingAndAreConfiguredAfterDrained`：DRAINING 变更照常、DRAINED 按策略

> 📁 完整测试代码：[DrainingBlockingQueueTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/src/test/java/io/github/huatalk/parallelinscope/queue/DrainingBlockingQueueTest.java)

## 总结

- **设计目标一句话**：把优雅关闭做成队列一等公民——不丢元素、生产端单向关门、消费端自然排干、关闭不被用户代码阻塞。
- **新契约**：关闭 ≠ 中断，也 ≠ 立即终结。`close()` 后写端拒绝，读端继续排干；排空即终态，此后才是毒丸 / `NoSuchElementException`。
- **关闭即谓词**：生命周期状态折叠进双管程 guard，等待者自己"看见"关闭退出，无线程注册表、无 interrupt。
- **排干线性化**：最后元素的取出与 DRAINED 发布原子一致，`awaitDrained()` 可依赖。
- **JDK 异常**：`IllegalStateException` / `NoSuchElementException`，不引入自定义类型。
