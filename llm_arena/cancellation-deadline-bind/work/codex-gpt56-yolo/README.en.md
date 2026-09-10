[**English**](README.md) | [**Chinese**](README.zh-CN.md)

# parallel-in-scope

[![CI](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml/badge.svg)](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.huatalk/parallel-in-scope.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.huatalk/parallel-in-scope)
[![Java 8+](https://img.shields.io/badge/Java-8%2B-007396?logo=openjdk&logoColor=white)](https://github.com/HuaTalk/parallel-in-scope#compatibility-and-build)
[![License](https://img.shields.io/github/license/HuaTalk/parallel-in-scope)](LICENSE)

> Current development version: `0.2.0-SNAPSHOT`. The `0.2.0` line is a breaking API migration from `0.1.x`.

A structured-concurrency toolkit for Java 8+ with bounded batch submission, cooperative cancellation, context propagation, and task-graph diagnostics.

## Quick Start

```xml
<dependency>
    <groupId>io.github.huatalk</groupId>
    <artifactId>parallel-in-scope</artifactId>
    <version>0.2.0</version>
</dependency>
```

Create the application execution topology once. A named `Par` is bound to its executor at build time; choose it before calling `map`, not per invocation.

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

`GlobalPar.close()` releases framework-owned timer and submitter resources. It deliberately does not shut down the executor services supplied to `register`; their owner must do that.

## v0.2 Migration

`ParConfig`, `ParOptions`, `GlobalParConfig`, `Par.getInstance()`, `new Par(...)`, and `Par.map(executorName, ...)` are removed. Use `GlobalPar`, `BatchExecutionOptions`, and `global.par(name).map(...)` instead. See the [v0.2 migration guide](docs/en/migration-v0.2.md) before upgrading an existing application.

## Core Capabilities

- Immutable application topology with multiple named, executor-bound `Par` entries
- Per-batch `BatchExecutionOptions` resolved into a `BatchExecutionContext`
- Fail-fast, timeout, manual, and parent-to-child cooperative cancellation
- Sliding-window submission with terminal results for tasks that were never submitted
- Cross-`Par` nested calls with task/executor identity graph recording
- Optional, threshold-driven cancellation purge per physical `ThreadPoolExecutor`
- Draining-close lifecycle queue `DrainingBlockingQueue` (consumers keep draining after close)

## Documentation

| Entry | Contents |
|---|---|
| [English documentation](docs/en/index.md) | Current user guide, migration, API contracts, and internals |
| [Chinese documentation](docs/zh/index.md) | Chinese-language documentation set |
| [Demo project](demo/README.en.md) | Runnable `0.1.x` compatibility examples; not a v0.2 API reference |

## Compatibility and Build

- Runtime: Java 8+
- Build tool: Maven 3.x
- Published artifact: root `parallel-in-scope` project

```bash
mvn clean verify
mvn install -DskipTests -Dmaven.javadoc.skip=true
```

## License

[Apache License 2.0](LICENSE)
