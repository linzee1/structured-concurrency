# Nullability Annotations


parallel-in-scope 使用混合空指针注解策略，在编译期和 IDE 中为开发者提供空值安全提示。

## 设计理由

### 为什么选择混合方案？

| 关注点 | JSR-305 (`javax.annotation`) | Checker Framework (`org.checkerframework`) |
|--------|------------------------------|---------------------------------------------|
| IDE 识别度 | IntelliJ、Eclipse、SpotBugs 全支持 | IntelliJ、Eclipse 支持 |
| 注解目标 | METHOD, PARAMETER, FIELD | TYPE_USE（可标注泛型参数） |
| 下游用户熟悉度 | 最高（业界事实标准） | 中等 |
| Guava 一致性 | Guava 老代码使用 | Guava 新代码方向 |

**结论**：面向下游用户的 Public API 和 SPI 使用 JSR-305（最大兼容性），内部实现使用 Checker Framework（`TYPE_USE` 更精确）。

### 为什么使用包级别默认 `@NonNull`？

通过在每个包的 `package-info.java` 中声明 `@ParametersAreNonnullByDefault`：
- 所有方法参数默认为 `@NonNull`，无需逐一标注
- 只需在少数可空的参数和返回值上标注 `@Nullable`
- 大幅减少标注噪音，提高代码可读性

## 规则总结

### 1. 包级别默认

每个包都有 `package-info.java`：

```java
@ParametersAreNonnullByDefault
package io.github.huatalk.parallelinscope.scope;

import javax.annotation.ParametersAreNonnullByDefault;
```

### 2. 注解来源选择

| 类别 | 注解来源 | import 语句 |
|------|---------|-------------|
| Public API 类 | JSR-305 | `import javax.annotation.Nullable;` |
| SPI 接口 | JSR-305 | `import javax.annotation.Nullable;` |
| Internal 类 | Checker Framework | `import org.checkerframework.checker.nullness.qual.Nullable;` |

**Public API 类**：`GlobalPar`, `Par`, `MultiTaskOptions`, `AsyncBatchResult`, `Checkpoints`, `TaskType`, `CancellationToken`, `CancellationToken.State`

**SPI 接口**：`TaskListener`, `DeadlockDetectionListener`

**Internal 类**：其余所有类

### 3. 什么时候需要标注 `@Nullable`

- **返回值可能为 null**：仅在 API 明确允许缺省值时标注
- **参数显式接受 null**：如 `CancellationToken` 的 parent 构造参数可传 `null`
- **构造器参数可选**：如 `AsyncBatchResult` 构造器的 `submitCanceller` 可以传 null

### 4. 什么时候不需要标注

- **参数不可为 null**：由包级别 `@ParametersAreNonnullByDefault` 覆盖，不需要显式 `@Nonnull`
- **返回值不可为 null**：不需要标注（大多数方法的默认情况）
- **字段**：不使用注解标注字段（由构造器保证）

### 5. Checker Framework TYPE_USE 风格

Internal 类使用 Checker Framework 注解时，`@Nullable` 放在类型前面（TYPE_USE 位置）：

```java
// Checker Framework 风格（Internal 类）
public static @Nullable ScopedCallable<?> current() { ... }
public static @Nullable Data data() { ... }
```

而 JSR-305 风格（Public API / SPI）放在方法声明前：

```java
// JSR-305 风格（Public API / SPI）
@Nullable
public ListeningExecutorService getExecutor(String name) { ... }
```

## 依赖配置

两个注解库在 `pom.xml` 中显式声明为 `provided` scope：

```xml
<dependency>
    <groupId>com.google.code.findbugs</groupId>
    <artifactId>jsr305</artifactId>
    <version>3.0.2</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.checkerframework</groupId>
    <artifactId>checker-qual</artifactId>
    <version>3.55.1</version>
    <scope>provided</scope>
</dependency>
```

`provided` scope 意味着：
- 编译时可用（IDE 提示正常工作）
- 不传递给下游用户（避免依赖冲突）
- Guava 已传递引入这两个库，`provided` 声明是防护性保障
