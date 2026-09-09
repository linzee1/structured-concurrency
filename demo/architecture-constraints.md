# Architecture Constraints - parallel-in-scope-demo

## 概述

demo 子项目是一个完全独立的示例模块，用于演示 parallel-in-scope 库的使用方法。

## 核心约束

### 1. 依赖方向（单向依赖）

```
demo (消费者) → parallel-in-scope (发布版本)
```

- **允许**: demo 依赖 parallel-in-scope 的发布版本（Maven Central）
- **禁止**: demo 依赖 parallel-in-scope 的源代码
- **禁止**: parallel-in-scope 依赖 demo

### 2. 包访问限制

#### 允许访问的包（公共 API）

| 包名 | 说明 |
|------|------|
| `io.github.monadrome.parallelinscope` | 核心 API、协作取消和监听回调 |
| `io.github.monadrome.parallelinscope.queue` | 独立的通用队列实现 |

执行内核与公开 API 同在根包，但内核类是 package-private，外部消费者无法编译依赖。
旧的 `.scope` / `.cancel` / `.context` / `.internal` / `.spi` / `.control` 包已移除。

### 3. 包命名约定

demo 子项目使用独立的包命名空间：

```
src/
├── main/java/demo/
│   ├── basic/          # 基础示例
│   ├── advanced/       # 高级示例
│   └── integration/    # 集成示例
└── test/java/demo/
    └── article/        # 文章配套测试
```

**禁止使用**: `io.github.monadrome.parallelinscope.*` 包名

### 4. 代码修改限制

- **禁止修改**: parallel-in-scope 主项目的任何文件
- **禁止依赖**: 主项目的 `src/` 目录
- **禁止**: 通过相对路径引用主项目代码

## 验证规则

### 自动验证

`ArchitectureConstraintsTest` 已检查主源码不引用已移除的旧包，并确保使用
`demo.*` 包命名空间。内核可见性由 Java 编译器强制；Maven 依赖边界仍需通过 POM 审查和
`dependency:tree` 验证。

### 手动验证

```bash
# 检查依赖
mvn dependency:tree

# 编译验证（确保不访问内部包）
mvn clean compile

# 运行测试
mvn test
```

## 架构图

```
┌─────────────────────────────────────────────────────────┐
│                    demo 子项目                           │
│                                                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐       │
│  │   basic/    │ │  advanced/  │ │ integration/│       │
│  │             │ │             │ │             │       │
│  │ BasicParDemo│ │ DeadlockDet │ │ BatchProcess│       │
│  │ Cancellation│ │             │ │             │       │
│  └─────────────┘ └─────────────┘ └─────────────┘       │
│                                                         │
└─────────────────────────────────────────────────────────┘
                            │
                            │ depends on (published artifact)
                            ▼
┌─────────────────────────────────────────────────────────┐
│              parallel-in-scope (发布版本)                │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │          根包：公共 API + callbacks             │   │
│  │  GlobalPar, Par, MultiTaskOptions, TaskGroup,        │   │
│  │  TaskBatchResult, CancellationToken, listeners...   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │       根包：package-private 执行内核          │   │
│  │  context, graph, submission, purge, phase state     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │       queue：独立通用队列（公共 API）          │   │
│  │  DrainingBlockingQueue, VariableLinkedBlockingQueue│   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 常见错误

### ❌ 错误示例

```java
// 错误 1: 内核类是 package-private，该 import 无法编译
import io.github.monadrome.parallelinscope.SlidingWindowSubmitter;

// 错误 2: 使用主项目包名
package io.github.monadrome.parallelinscope.demo;  // 应该是 demo.basic

// 错误 3: 依赖源代码
// pom.xml 中使用 systemPath 指向主项目
```

### ✅ 正确示例

```java
// 正确 1: 只访问公共 API
import io.github.monadrome.parallelinscope.Par;
import io.github.monadrome.parallelinscope.MultiTaskOptions;

// 正确 2: 使用独立包名
package demo.basic;

// 正确 3: 依赖发布版本
// pom.xml 中使用 Maven Central 坐标
```

## 维护指南

1. **添加新示例**: 创建在 `src/main/java/demo/basic/`、`src/main/java/demo/advanced/` 或 `src/main/java/demo/integration/` 包下
2. **更新依赖**: 只更新 parallel-in-scope 的版本号
3. **架构验证**: 定期运行 `mvn clean compile` 确保不违反约束
