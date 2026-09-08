[**English**](README.md) | [**中文**](README.zh-CN.md)

# parallel-in-scope

[![CI](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml/badge.svg)](https://github.com/HuaTalk/parallel-in-scope/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.huatalk/parallel-in-scope.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.huatalk/parallel-in-scope)
[![Java 8+](https://img.shields.io/badge/Java-8%2B-007396?logo=openjdk&logoColor=white)](https://github.com/HuaTalk/parallel-in-scope#compatibility-and-build)
[![License](https://img.shields.io/github/license/HuaTalk/parallel-in-scope)](LICENSE)

> Online documentation: [huatalk.github.io/parallel-in-scope](https://huatalk.github.io/parallel-in-scope/)
>
> Current version: `v0.2.0`. APIs may still change in future `0.x` releases.

A structured-concurrency toolkit for Java 8+ with cooperative cancellation, fail-fast execution, context propagation, sliding-window scheduling, and thread-pool deadlock diagnostics.

## Quick Start

```xml
<dependency>
    <groupId>io.github.huatalk</groupId>
    <artifactId>parallel-in-scope</artifactId>
    <version>0.2.0</version>
</dependency>
```

```java
GlobalPar execution = GlobalPar.builder()
        .register("io", Executors.newFixedThreadPool(8))
        .build();

MultiTaskOptions options = MultiTaskOptions.of("fetch-user")
        .parallelism(4)
        .timeout(Duration.ofSeconds(3))
        .build();

TaskBatchResult<User> result = execution.par("io")
        .map(userIds, userService::findById, options);
```

## Core Capabilities

- Fail-fast cancellation within a task batch
- Timeout, explicit, and parent-to-child cancellation propagation
- Sliding-window submission with bounded concurrency
- Cross-thread `ThreadLocal` context propagation
- CPU / IO task-aware scheduling
- Monitoring SPI for execution, queueing, and failures
- Cycle detection across task and executor graphs

## Documentation

| Entry | Contents |
|---|---|
| [English documentation](docs/en/index.md) | User guides, API references, design notes, and case studies |
| [v0.2 migration guide](docs/en/migration-v0.2.md) | Breaking changes from the `0.1.x` API |
| [Full user guide](docs/en/user-guide.md) | Configuration, API usage, execution flow, and advanced features |
| [Demo project](demo/README.en.md) | Runnable examples and the article catalog |
| [Chinese documentation](docs/zh/index.md) | Complete Chinese documentation set |

## Compatibility and Build

- Runtime: Java 8+
- Build tool: Maven 3.x
- Published artifact: root `parallel-in-scope` project
- Examples: independent `demo/` project, not published

```bash
mvn clean verify
mvn install -DskipTests -Dmaven.javadoc.skip=true
mvn -f demo/pom.xml test
```

## License

[Apache License 2.0](LICENSE)
