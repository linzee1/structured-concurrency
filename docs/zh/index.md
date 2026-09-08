# parallel-in-scope 文档

这里汇总使用指南、API 参考、设计文档和示例。

## 使用指南

| 文档 | 内容 |
|---|---|
| [完整使用指南](user-guide.md) | 配置、公共 API、执行语义和进阶能力 |
| [v0.2 迁移指南](migration-v0.2.md) | 从 `0.1.x` 迁移到当前 API |
| [5 分钟快速上手](https://github.com/HuaTalk/parallel-in-scope/blob/main/demo/docs/zh-CN/articles/QUICK-START-5-minutes.md) | 用最小示例完成首次调用 |
| [批量调用最佳实践](https://github.com/HuaTalk/parallel-in-scope/blob/main/demo/docs/zh-CN/articles/BATCH-best-practices.md) | HTTP、数据库和混合 IO 场景 |
| [Demo 文档索引](https://github.com/HuaTalk/parallel-in-scope/blob/main/demo/docs/zh-CN/README.md) | 示例：问题复现、解决方案和可执行测试 |

## API 与契约参考

| 文档 | 内容 |
|---|---|
| [协作式取消](reference/cooperative-cancellation.md) | checkpoint、异常与取消传播 |
| [Nullability Annotations](reference/nullability-annotations.md) | 空值注解规则和依赖约定 |

## 实现原理与设计决策

| 文档 | 内容 |
|---|---|
| [并发库的减法哲学](design/philosophy.md) | 核心取舍与边界 |
| [Idea Graveyard](design/idea-graveyard.md) | 明确不提供的能力及替代方案 |
| [架构与数据链路](visuals/parallel-in-scope-architecture.html) | 执行、取消、调度和检测链路 |
| [交互式用户指南](visuals/parallel-in-scope-user-guide.html) | 主要使用路径与 FAQ |

## 测试与案例

| 文档 | 内容 |
|---|---|
| [TaskBatchResult 测试设计](testing/task-batch-result-report-test-design.md) | 稳定契约与并发断言 |
