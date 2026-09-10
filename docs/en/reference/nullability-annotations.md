# Nullability Annotations

`parallel-in-scope` uses a mixed nullability strategy to provide useful compiler and IDE feedback without adding runtime dependencies to consumers.

## Design

Public APIs and SPI interfaces use JSR-305 annotations for broad IDE compatibility. Internal implementation classes use Checker Framework annotations, whose `TYPE_USE` support is more precise for generic types.

Every package declares `@ParametersAreNonnullByDefault` in `package-info.java`. Only nullable parameters and return values need explicit `@Nullable`.

## Annotation sources

| Area | Annotation source | Examples |
|---|---|---|
| Public API | JSR-305 | `GlobalPar`, `Par`, `BatchOptions`, `TaskGroupOptions`, `TaskOptions`, `TaskBatchResult`, `Checkpoints` |
| Callbacks | JSR-305 | `TaskListener`, `TaskGroupListener`, `DeadlockDetectionListener` |
| Internal implementation | Checker Framework | Executor, queue, context, and graph internals |

## When to use `@Nullable`

Use it when a return value can be absent, when a public constructor explicitly accepts `null`, or
when a parameter has an optional value. Public examples include a nullable parent
`CancellationToken` and `TaskBatchResult.BatchReport.firstException()`. Package-private runtime
types use the same rule for their implementation state.

Do not add redundant annotations to non-null parameters covered by the package default or to non-null return values guaranteed by the implementation.

## Annotation styles

Checker Framework `TYPE_USE` style:

```java
static @Nullable TaskExecutionContext current() { ... }
```

This illustrates internal source style; `TaskExecutionContext` is not public API.

JSR-305 method style:

```java
@Nullable
public Throwable firstException() { ... }
```

## Dependencies

The `jsr305` and `checker-qual` artifacts are declared with `provided` scope. They are available for compilation and IDE analysis but are not forced onto downstream runtime classpaths.
