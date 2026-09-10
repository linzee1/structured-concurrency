# PR 文档与代码一致性分析

## 范围

仅核对 `origin/main...dev/v0.2.0` 已提交的 PR 差异；未将工作区未提交修改计入结论。

## 已确认不一致

1. `docs/en/reference/cooperative-cancellation.md:61` 与
   `docs/zh/reference/cooperative-cancellation.md:121` 的“不要吞掉取消”示例仍调用
   `par.map("myExecutor", items, ...)`。当前 `Par` 只提供
   `map(List, Function, BatchExecutionOptions)`（`src/main/java/io/github/huatalk/parallelinscope/scope/Par.java:85`），
   执行器名参数已在 v0.2 移除。因此两段示例无法编译。
   应改为已选定、绑定执行器的 `Par`（例如 `global.par("myExecutor").map(items, ...)`），或在示例前声明该 `Par`。

2. `README.en.md:71` 与 `README.zh-CN.md:71` 声称生命周期队列在 Java 21+ 暴露
   “sequenced/reverse-view” 行为。PR 提交 `baf018b` 已从 `DrainingBlockingQueue` 删除
   `addFirst`、`addLast`、`removeFirst`、`removeLast`、`getFirst`、`getLast` 等
   SequencedCollection 端点；当前实现也没有 `reversed()`。该能力描述已经失效，应删除或替换为实际公开 API。

## 已排除

- `docs/en/user-guide.md` 和 `docs/zh/user-guide.md` 的 v0.2 主路径、取消、队列说明与当前
  `GlobalPar`、`Par`、`BatchExecutionOptions`、`DrainingBlockingQueue` 实现一致。
- `docs/zh/design/philosophy.md`、`docs/zh/design/idea-graveyard.md` 以及 demo 文章中的旧
  `ParConfig`/`ParOptions`/按名称 `map` 示例已经明确标为历史 API，不作为当前 API 文档错误报告。
- 英文用户指南的“完整契约”链接指向中文设计文档，但路径可解析；这是语言选择，不是断链。
