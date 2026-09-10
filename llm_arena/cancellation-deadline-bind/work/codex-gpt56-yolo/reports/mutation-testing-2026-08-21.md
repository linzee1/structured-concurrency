# 变异测试报告（2026-08-21，补测后最终版）

## 运行方式

```bash
# 全量变异测试（约 14 分钟，8 worker）
mvn test-compile   # 注意：default-cli 直接调 goal 不触发编译，先编译
mvn -Ppitest org.pitest:pitest-maven:mutationCoverage

# 报告位置
# HTML: target/pit-reports/index.html
# 数据: target/pit-reports/mutations.xml
```

配置见 `pom.xml` 的 `pitest` profile：
- pitest-maven 1.19.1 + pitest-junit5-plugin 1.2.3，JUnit 5
- `timeoutConstant=20000`（测试合法等待上限 10s，防止并发测试被误判为超时击杀）
- `coverageTimeout=300`（覆盖阶段需跑完整套件，默认 10s 会误杀运行中的测试）
- 未启用 withHistory（增量缓存会按类复用旧测试集的结果，修改测试后产生陈旧数据）
- Maven 需运行在 JDK ≥ 11（PITest 要求），本机使用 JDK 21

## 补测前后对比

| 指标 | 补测前 | 补测后 |
|---|---|---|
| 测试数量 | 168 | **274**（+106） |
| 变异总数 | 1176 | 1176 |
| 击杀（KILLED） | 639 | **839** |
| 超时击杀（TIMED_OUT） | 26 | **29** |
| 存活（SURVIVED） | 162 | **122** |
| 无覆盖（NO_COVERAGE） | 349 | **186** |
| **变异得分** | **56.5%** | **73.8%**（+17.3pp） |

## 新增测试清单（按优先级）

1. **`VariableLinkedBlockingQueueTest`（59 个，新文件）** — 最大缺口：该队列类原先零直接测试（134 个无覆盖变异）。
   - 容量调整（setCapacity 正/零/负、缩容/扩容）、集合构造、FIFO 语义、peek/remainingCapacity
   - 阻塞-唤醒链：put/timedOffer 阻塞后被 take/poll/remove/drainTo/clear/扩容唤醒（击杀 signal 变异）
   - **锁泄漏探测（16 个）**：每个方法变异掉 unlock/fullyUnlock 后，另一线程调用同锁方法必须能完成，否则击杀
   - 弱一致迭代器：快照返回、外部 clear 存活、遍历中删除跳过
2. **`ThreadRelayTest`（+9 个）** — taskName/executorName 的默认值（新线程断言，避免测试残留）、设置/读取往返、null 回退
3. **`ScopedCallableTest`（+8 个）** — executorName 保留/null 回退、toString、执行期间 ThreadRelay 上下文填充、取消 token 触发 checkpoint、enqueued 阈值分类（3ms 边界 + 非整毫秒）
4. **`CheckpointsTest`（+15 个）** — 等待方法的成功/超时返回值断言、等待真实阻塞（CountDownLatch 同步模式）、checkJoin/checkSleep 实际等待、checkTryAcquire/checkTryLock/checkAwaitTermination 双路径
5. **`LeanCancellationExceptionTest`（3 个，新文件）** — 空堆栈契约、fillInStackTrace 返回自身
6. **`ParTest`（+1 个）** — TaskGraph fork 边记录 + 边元数据携带命名 executor
7. **顺手项** — `SmartBlockingQueueTest`（+2：构造边界、满队列 offer=false）、`ParConfigTest`（3 个新文件：默认超时/livelock 开关）、`LivelockListenerTest`（5 个新文件：hasAnyIssue 组合/toString）、`ParOptionsTest`（+1：parallelism=0 规范化）

## 剩余存活变异分析（122 个）

### 等价变异（不可杀，无需处理）

| 位置 | 原因 |
|---|---|
| Checkpoints 各方法的首行 `checkCancellationToken` 移除（约 12 个） | 内层转发方法重复检查，移除外层行为不变 |
| `LeanCancellationException` 构造中 `setStackTrace` 移除 | `fillInStackTrace()` 重写返回 `this`，堆栈本就为空 |
| `VariableLinkedBlockingQueue.clear` 循环条件取反 | `head = last` 仍执行，旧节点不可达，可观测行为一致 |
| `offer/put` 的 `c+1 < capacity` 边界/数学变异（L185/L207/L228） | 多 signal 一次（无等待者）或唤醒链被 put/timedOffer 的对应 signal 覆盖 |
| `take/poll` 的 `c > 1` 边界（L249/L272/L292） | c==1 时队列仍有元素，无等待者，signal 无效果 |
| `return true→true` / `return false→false` 注入（L320/323/342/344 等） | PITest 对常量 return 生成的无变化变异 |
| `ThreadRelay.setCurrentParallelOptions` 条件取反 | PARALLEL_OPTIONS 只写不读（无消费路径） |
| 各 `*Demo` 类的全部变异（~108 个无覆盖） | demo 类无测试属预期，可在 profile 中 `excludedClasses` 排除 |

### 剩余真缺口（低优先级，未处理）

| 位置 | 说明 |
|---|---|
| `ClosableBlockingQueue` 反向视图 ReverseView/ReverseItr（30 个） | addAll/removeIf/retainAll 返回值与边界变异；`access$` 辅助调用移除（需反射级验证） |
| `ConcurrentLimitExecutor.submitRemaining`（5 个） | 提交计数/返回值变异；interrupt 调用移除 |
| `TaskGraph`（5 个） | destroyAfterRequest 的 TTL remove、buildDetectionEvent 分支、canDeadlock 边界 |
| `Par`（4 个） | 父令牌链分支（需 token parent 断言 API）、tryPurgeOnTimeout（超时路径难观测） |
| `ScopedCallable` L180/181（2 个） | ThreadRelay token/options 设置移除（token 无读取 API） |
| `ClosableBlockingQueue` 本体（19 个） | 阻塞计数（endBlockingCall 位运算）、Lifecycle 发布调用、awaitRunning/Terminated 转发 |

## 过程中发现的问题（已修复）

1. **PITest 增量缓存陷阱**：`withHistory` 的缓存键只含测试类集合（不含方法体），修改测试后旧结果被复用（表现为变异 tests=0）。已禁用。
2. **`mvn -Ppitest ...` 不触发编译**：default-cli 直接调 goal 跳过 compile 阶段，PITest 用陈旧 class。运行前需 `mvn test-compile`。
3. **阻塞探测的线程状态误判**：线程池空闲线程在 `workQueue.take()` 时也是 `WAITING`，仅凭线程状态无法区分"已阻塞"与"空闲"；修复为 `!future.isDone() && WAITING`，Checkpoints 类改用 CountDownLatch 同步模式。
4. **覆盖阶段超时**：默认 10s 不够跑完整套件（92 类），minion 被杀后误报正在运行的测试失败；`coverageTimeout=300`。

## 备注

- 测试基线有一次 flaky 失败记录（复跑全绿），并发测试的不稳定性会影响变异测试可信度。
- TIMED_OUT=29 全部为合理击杀（变异破坏阻塞语义后测试等满期限被杀），非误杀。
- 剩余无覆盖 186 中 ~108 个来自 3 个 demo 类，其余为 SPI 辅助类与 ClosableBlockingQueue 内部视图。
