[**English**](README.md) | [**中文**](README.zh-CN.md)

# parallel-in-scope

[![CI](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml/badge.svg)](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.huatalk/parallel-in-scope.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.huatalk/parallel-in-scope)
[![Java 8+](https://img.shields.io/badge/Java-8%2B-007396?logo=openjdk&logoColor=white)](https://github.com/HuaTalk/parallel-in-scope#compatibility-and-build)
[![License](https://img.shields.io/github/license/HuaTalk/parallel-in-scope)](LICENSE)

> 当前开发版本：`0.2.0-SNAPSHOT`。`0.2.0` 相对 `0.1.x` 是一次破坏性 API 迁移。

面向 Java 8+ 的结构化并发工具包，提供有界批量提交、协作式取消、上下文传播和任务图诊断。

## 快速开始

```xml
<dependency>
    <groupId>io.github.huatalk</groupId>
    <artifactId>parallel-in-scope</artifactId>
    <version>0.2.0</version>
</dependency>
```

在应用启动阶段构建一次执行拓扑。具名 `Par` 在构建期绑定执行器；先选择 `Par`，再调用 `map`，而不是每次调用时按名称选池。

```java
GlobalPar global = GlobalPar.builder()
        .register("io", Executors.newFixedThreadPool(8))
        .defaultPar("io")
        .build();

BatchExecutionOptions options = BatchExecutionOptions.of("fetch-user")
        .taskType(TaskType.IO_BOUND)
        .parallelism(4)
        .timeout(Duration.ofSeconds(3))
        .build();

AsyncBatchResult<User> result = global.par("io")
        .map(userIds, userService::findById, options);
```

`GlobalPar.close()` 只释放框架自建的 timer 和 submitter 资源，不会关闭传给 `register` 的执行器；执行器生命周期仍由应用负责。

## v0.2 迁移

`ParConfig`、`ParOptions`、`GlobalParConfig`、`Par.getInstance()`、`new Par(...)` 和 `Par.map(executorName, ...)` 均已移除。请改用 `GlobalPar`、`BatchExecutionOptions` 与 `global.par(name).map(...)`。升级既有应用前请阅读 [v0.2 迁移指南](docs/zh/migration-v0.2.md)。

## 核心能力

- 管理多个具名、构建期绑定执行器的不可变 `GlobalPar`
- 每批调用的 `BatchExecutionOptions` 解析为 `BatchExecutionContext`
- 快速失败、超时、手动取消和父子批次协作式取消
- 滑动窗口提交；未提交任务也会获得终态结果
- 跨 `Par` 的嵌套调用及以执行器 identity 为基础的任务图记录
- 基于阈值、按物理 `ThreadPoolExecutor` 合并的取消任务 purge
- 排干式关闭的生命周期队列 `DrainingBlockingQueue`（关闭后消费端继续取走存量直到排空）

## 文档

| 入口 | 内容 |
|---|---|
| [中文文档中心](docs/zh/index.md) | 当前使用指南、迁移、API 契约与内部原理 |
| [English documentation](docs/en/index.md) | 英文文档集 |
| [Demo 工程](demo/README.md) | `0.1.x` 兼容示例，不是 v0.2 API 参考 |

## 兼容性与构建

- 运行时：Java 8+
- 构建工具：Maven 3.x
- 发布产物：根项目 `parallel-in-scope`

```bash
mvn clean verify
mvn install -DskipTests -Dmaven.javadoc.skip=true
```

## License

[Apache License 2.0](LICENSE)
