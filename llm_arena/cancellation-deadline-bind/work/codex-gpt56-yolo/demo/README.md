**中文** | [**English**](README.en.md)

# parallel-in-scope-demo

独立示例项目，演示如何使用 parallel-in-scope 并发工具库。当前代码和文章统一使用 `0.2.0-SNAPSHOT` API，可作为外部消费者示例。

## 快速开始

### 1. 编译项目

```bash
cd demo
mvn clean compile
```

### 2. 运行示例

```bash
# 运行基础示例
mvn exec:java

# 运行特定示例
mvn exec:java -Dexec.mainClass=demo.basic.BasicParDemo
mvn exec:java -Dexec.mainClass=demo.basic.CancellationDemo
mvn exec:java -Dexec.mainClass=demo.advanced.DeadlockDetectionDemo
mvn exec:java -Dexec.mainClass=demo.integration.BatchProcessingDemo
```

### 3. 运行测试

```bash
mvn test
```

### 4. 使用脚本运行

```bash
chmod +x scripts/run-demos.sh
./scripts/run-demos.sh basic
./scripts/run-demos.sh all
./scripts/run-demos.sh test
```

## 示例列表

| 示例 | 包 | 说明 |
|------|-----|------|
| BasicParDemo | basic | Par.map() 基本用法 |
| CancellationDemo | basic | 任务超时取消机制 |
| DeadlockDetectionDemo | advanced | 死锁检测功能 |
| BatchProcessingDemo | integration | 批量数据处理 |

## 架构约束

这个示例项目是完全独立的，只依赖 parallel-in-scope 的发布版本。

### 依赖方向

```
demo (消费者) → parallel-in-scope (发布版本)
```

### 包访问限制

**允许访问**：
- `io.github.huatalk.parallelinscope.scope` (GlobalPar, Par, BatchExecutionOptions, AsyncBatchResult)
- `io.github.huatalk.parallelinscope.spi` (TaskListener, DeadlockDetectionListener)

**禁止访问**：
- `io.github.huatalk.parallelinscope.internal`
- `io.github.huatalk.parallelinscope.cancel`
- `io.github.huatalk.parallelinscope.context`
- `io.github.huatalk.parallelinscope.context.graph`
- `io.github.huatalk.parallelinscope.queue`

### 包命名约定

使用 `demo.*` 命名空间，禁止使用 `io.github.huatalk.parallelinscope.*`

## 添加新示例

1. 选择合适的包（basic/advanced/integration）
2. 创建新的 Java 类，使用 `demo.*` 包名
3. 只导入允许的包（scope 和 spi）
4. 添加对应的测试类
5. 更新 `scripts/run-demos.sh` 脚本

## 常见问题

### Q: 为什么不能访问内部包？

A: 内部包是 parallel-in-scope 的实现细节，可能在版本更新时发生变化。只使用公共 API 可以确保示例代码的稳定性。

### Q: 如何更新依赖版本？

A: 修改 `pom.xml` 中的 `parallel-in-scope.version` 属性。

### Q: 如何验证架构约束？

A: 运行架构约束测试：
```bash
mvn test -Dtest=ArchitectureConstraintsTest
```

## 相关文档

- [Demo 文章索引](docs/zh-CN/README.md)
- [项目中文文档中心](../docs/zh/index.md)
- [parallel-in-scope 主项目](../README.md)
- [架构约束详细说明](architecture-constraints.md)
- [CLAUDE.md](CLAUDE.md)
