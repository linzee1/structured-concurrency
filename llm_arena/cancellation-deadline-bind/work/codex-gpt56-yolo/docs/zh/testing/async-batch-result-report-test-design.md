# AsyncBatchResult 报告能力测试设计


## 1. 目标

本测试集验证 `AsyncBatchResult.report()` 和 `reportString()` 对批次当前状态的真实表达能力，并验证 `Par.map()` fail-fast 后的报告仍满足公开契约。

测试不把线程调度产生的偶然状态数量当作产品契约。凡是断言精确数量的场景，都必须先通过已完成 Future 或显式同步原语建立确定状态。

## 2. 被测能力

| 能力 | 稳定契约 | 测试层级 |
|---|---|---|
| 状态分类 | 每个 Future 被归入 `RUNNING`、`SUCCESS`、`FAILED`、`CANCELLED` 之一 | 核心单元测试 |
| 状态计数 | 所有状态计数之和等于结果 Future 数量 | 单元测试、集成测试 |
| 首异常 | 返回 results 列表中第一个失败 Future 的 cause；没有失败时为 null | 核心单元测试 |
| 快照语义 | `report()` 反映调用时状态；Future 后续完成时需再次调用以获取新快照 | 核心单元测试 |
| 报告不可变 | 已生成报告的状态 map 不允许调用方修改 | 核心单元测试 |
| 文本摘要 | 按状态枚举顺序输出计数；有失败时追加首异常消息 | 核心单元测试 |
| 全成功批次 | 所有结果成功、无首异常、摘要只有 `SUCCESS` | Demo 集成测试 |
| fail-fast 批次 | 首个失败成为根因，未完成 sibling 被取消，最终报告覆盖全部输入 | Demo 集成测试 |

## 3. 测试分层

### 3.1 核心单元测试

直接使用 Guava 的 immediate/settable future 构造状态，不启动线程池。

这层负责精确验证：

- 混合状态的准确数量；
- 首异常按结果列表顺序选择，而不是按完成先后选择；
- `RUNNING` 到 `SUCCESS` 的快照变化；
- 空批次行为；
- 状态 map 不可变；
- `reportString()` 的完整格式。

这些测试失败时，原因应局限于报告聚合逻辑，不受调度器、线程池或取消传播影响。

### 3.2 Demo 集成测试

通过公共 `Par` API 验证用户真正观察到的批次报告。

全成功场景可以断言精确的 `SUCCESS:N`。fail-fast 场景使用 `CountDownLatch` 建立以下顺序：

1. 一个 sibling 已进入阻塞状态；
2. 指定任务抛出根异常；
3. fail-fast 取消阻塞 sibling 和尚未运行的任务；
4. 等待所有 Future 进入终态后生成报告。

集成测试的首要不变量是：

```text
所有状态计数之和 == 输入任务数
至少存在一个 FAILED
firstException == 触发 fail-fast 的根异常
```

即使场景通过 latch 得到确定终态，也不把未经控制的普通任务执行速度作为断言前提。

## 4. 不采用的测试方式

### 固定普通并发任务的成功/失败数量

例如输入中有 4 个函数会抛异常，并不意味着 fail-fast 批次最终必有 4 个 `FAILED`。后续任务可能在执行到异常之前已经被取消。

### 使用 sleep 排列线程时序

sleep 只能提高某种执行顺序出现的概率，不能建立 happens-before 关系。测试使用 latch、future 完成状态或直接构造 Future。

### 只断言字符串包含若干片段

文本断言不能替代结构化状态验证。核心测试先验证 `BatchReport`，再验证字符串是该结构的稳定表示。

### 为了得到 6/4 而吞掉任务异常

把异常包装为业务返回值会阻止 fail-fast，但此时 Future 状态全部是 `SUCCESS`，测试的是业务结果建模，而不是 `AsyncBatchResult` 的失败报告能力。

## 5. 测试用例清单

| 用例 | 场景 | 关键断言 |
|---|---|---|
| mixed terminal states | success + two failures + cancellation | 精确计数、列表首失败、完整字符串 |
| running snapshot | settable future 未完成后再完成 | 旧报告不变，新报告更新 |
| empty batch | 空 Future 列表 | 空 map、null 异常、空字符串 |
| immutable report | 修改 stateCounts | 抛 `UnsupportedOperationException` |
| all-success integration | `Par.map()` 全部返回 | 总数 N、无异常、`SUCCESS:N` |
| controlled fail-fast integration | 根失败 + 阻塞 sibling + 待提交任务 | 总数守恒、失败根因、存在取消 |

## 6. 维护准则

- 新增状态时，同时更新 `FutureState` 分类测试和文本顺序测试。
- 更改首异常选择策略时，必须明确是“列表顺序”还是“完成顺序”，并更新 API 文档。
- 并发测试若出现偶发失败，首先检查是否缺失同步关系，不得通过增加 sleep 或重试掩盖。
- Demo 测试只使用公共 API；内部枚举的精确断言放在主项目测试中。
- 验证至少包括主项目 clean test、demo clean test，以及目标并发测试的重复运行。
