# 设计文档路由

使用规则：**先读本表，按摘要匹配当前任务，只加载命中的文档，不要预读全部。**
契约类文档以实现约束力（MUST/MUST NOT/SHOULD）书写，是本仓库行为的权威依据。

## TaskGroup（独立并行任务组）

| 文档 | 摘要 |
|---|---|
| [task-group-api-and-options.md](task-group-api-and-options.md) | TaskGroup 目标与非目标、Group/Batch 语义边界、`TaskGroupDefinition`/`TaskKey`/`TaskGroup` 公共 API、选项类型（`BatchOptions`/`TaskGroupOptions`/`TaskOptions`）、结果类型（`TaskGroupResult`/`TaskOutcome`） |
| [task-group-lifecycle.md](task-group-lifecycle.md) | TaskGroup 对象与上下文生命周期（MemberState、TaskExecutionContext、SubmissionScope、TTL 边界）、结构 parent/取消 parent/deadline 解耦、状态机与完成原因、GlobalPar 关闭与资源所有权 |
| [task-group-submission.md](task-group-submission.md) | TaskGroup submit 冻结与统一提交契约、配置期校验、executor rejection、两阶段提交内核 `TaskSubmissions` 的复用边界 |
| [task-group-cancellation.md](task-group-cancellation.md) | TaskGroup 取消 token 拓扑、成员主动取消级联、fail-fast、deadline 计算与 timer、成员 bind 跳过策略、`originState()` 归因规则 |
| [task-group-observability-and-verification.md](task-group-observability-and-verification.md) | TaskGroup 成员 TaskListener 与 `TaskGroupListener`、TaskGraph 规则、并发不变量、必测矩阵、验收标准 |
| [task-group-terminal-combine.md](task-group-terminal-combine.md) | 可选的单一终端汇合任务：API、全量 join、结果、取消、观测、缺点与非目标 |
| [group-api-redesign-v0.3-proposal.md](group-api-redesign-v0.3-proposal.md) | （提案，待讨论）TaskGroup 用户接口重新设计：概念清点、公理推导、目标 API 与旧新映射；通过后沉淀回契约系列 |
| [enhanced-listenablefuture-analysis.md](enhanced-listenablefuture-analysis.md) | （分析，待讨论）增强 ListenableFuture：内核现有增强盘点、future 通道缺口、公开 `TaskFuture` 评估与实现约束；v0.3 group 提案的前置分析 |
| [task-future-design.md](task-future-design.md) | （提案，待讨论）`Task`/`TaskFuture` 设计契约：forwarding 模式增强 future 的目标、约束、不变量、API、实现方式与能力取舍 |
| [handoff-task-future-implementation.md](handoff-task-future-implementation.md) | （交接）Task/TaskFuture → group v0.3 的实施交接：已定决策及理由、代码锚点、实施步骤、测试矩阵、坑位清单；供零上下文实现会话直接开工 |

## 取消与队列

| 文档 | 摘要 |
|---|---|
| [cancellation-propagation.md](cancellation-propagation.md) | Guava `ListenableFuture` 取消传播机制（transform/catching/addCallback/组合 future 的方向差异），`CancellationToken.bind` 依赖的语义与源码索引 |
| [draining-queue-contract.md](draining-queue-contract.md) | `DrainingBlockingQueue` 逐渐关闭契约：OPEN→DRAINING→DRAINED 状态机、规则优先级瀑布、poison/mutations 配置 |

## 扩展与包装

| 文档 | 摘要 |
|---|---|
| [extension-and-wrapping.md](extension-and-wrapping.md) | 用户扩展接缝的唯一位置（任务体 `Callable` 层、上下文层之内）、三个前提与三个不变量（I1 结构 / I2 同步动态范围 / I3 只能检测）、三个包装轴（线程池/Callable/FutureTask）、必须避免的 19 类问题、契约与验证矩阵 |

## 设计哲学与决策记录

| 文档 | 摘要 |
|---|---|
| [first-principles.md](first-principles.md) | 项目公理层：结构化并发出发点、推导出的核心决策、新需求评估判据；评估任何新特性/新概念先对照本文 |
| [docs/zh/design/philosophy.md](../docs/zh/design/philosophy.md)（[en](../docs/en/design/philosophy.md)） | 并发库的减法哲学：核心取舍与边界，评估新特性是否契合项目定位（已发布站点页面，保留在原位置） |
| [docs/zh/design/idea-graveyard.md](../docs/zh/design/idea-graveyard.md)（[en](../docs/en/design/idea-graveyard.md)） | 明确不提供的能力及替代方案，引入新特性前先查否决记录（已发布站点页面，保留在原位置） |
| [adr/](../adr/) | 架构决策记录（不可变；过时决策以 Superseded 标注）。注意 ADR 是历史快照，现行契约以本目录 `design/` 为准 |
