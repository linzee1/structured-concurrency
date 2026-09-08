# Nullability Annotations

`parallel-in-scope` uses a mixed nullability strategy to provide useful compiler and IDE feedback without adding runtime dependencies to consumers.

## Design

Public APIs and SPI interfaces use JSR-305 annotations for broad IDE compatibility. Internal implementation classes use Checker Framework annotations, whose `TYPE_USE` support is more precise for generic types.

Every package declares `@ParametersAreNonnullByDefault` in `package-info.java`. Only nullable parameters and return values need explicit `@Nullable`.

## Annotation sources

| Area | Annotation source | Examples |
|---|---|---|
| Public API | JSR-305 | `GlobalPar`, `Par`, `MultiTaskOptions`, `AsyncBatchResult`, `Checkpoints` |
| SPI | JSR-305 | `TaskListener`, `DeadlockDetectionListener` |
| Internal implementation | Checker Framework | Executor, queue, context, and graph internals |

## When to use `@Nullable`

Use it when a return value can be absent, when a constructor explicitly accepts `null`, or when a parameter has an optional value. Examples include a nullable parent `MultiTaskContext`, a nullable parent `CancellationToken`, and an optional `submitCanceller` in `AsyncBatchResult`.

Do not add redundant annotations to non-null parameters covered by the package default or to non-null return values guaranteed by the implementation.

## Annotation styles

Checker Framework `TYPE_USE` style:

```java
public static @Nullable ScopedCallable<?> current() { ... }
```

JSR-305 method style:

```java
@Nullable
public ListeningExecutorService getExecutor(String name) { ... }
```

## Dependencies

The `jsr305` and `checker-qual` artifacts are declared with `provided` scope. They are available for compilation and IDE analysis but are not forced onto downstream runtime classpaths.
