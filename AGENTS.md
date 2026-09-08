# AGENTS.md

## Project

**parallel-in-scope** is a structured-concurrency toolkit for Java 8+ built on
Guava `ListenableFuture` and Alibaba `TransmittableThreadLocal`.

- Maven coordinates: `io.github.huatalk:parallel-in-scope:0.2.0`
- Java source/target 1.8 (tests compiled with release 11); JUnit 5 via Surefire

## Commands

Run from the repository root.

```bash
mvn test                                      # all tests
mvn test -Dtest=<ClassName>#<methodName>      # targeted test
mvn spotless:apply                            # format
mvn clean verify                              # tests + package checks
```

## Architecture

Base package: `io.github.huatalk.parallelinscope`.

| Package | Responsibility |
|---|---|
| `scope` | API facade |
| `cancel` | Cancellation subsystem |
| `context` | TTL/TL context propagation and the observation scope lifecycle |
| `context.graph` | Task graph data (`TaskGraphData`) and edge metadata |
| `internal` | Execution engine |
| `queue` | Scheduling queues |
| `spi` | Extension points (listeners, phases) |

Two invariants to respect:

- `CancellationToken.bind()` wires deadline, fail-fast, and parent
  propagation only after all futures are submitted; the deadline itself lives
  in the token (min of the requested deadline and the parent's).
- `SlidingWindowSubmitter.submitAll()` returns the exact prepared
  `ExecutionPhaseHintFuture` for tasks in the initial parallelism window;
  tasks beyond the window are returned as `SettableFuture` placeholders
  bridged via `setFuture()` when a slot frees.

## Key Conventions

- Java 8 APIs only in `src/main/java`.
- Accessors use the bare `x()` style everywhere (`token.state()`, `event.result()`);
  do not introduce `getX()`/`isX()` forms. Methods implementing JDK or
  third-party contracts keep their mandated names (`ExecutorService.isShutdown()`,
  `Monitor.Guard.isSatisfied()`).
- Every package has `package-info.java` with `@ParametersAreNonnullByDefault`;
  annotate only exceptions with `@Nullable` — `javax.annotation.Nullable` for
  public API/SPI, `org.checkerframework.checker.nullness.qual.Nullable` for
  internal code (both provided scope).
- Logging goes through JUL (`java.util.logging.Logger`).
- The `Scope` suffix marks a closeable lifecycle scope (`SubmissionScope`,
  `TaskGraphObservationScope`); the `Context` suffix marks a data carrier
  (a view or resolved parameters).
- Pre-stable API: public APIs and SPI may change between `0.x` releases without
  compatibility shims. During the `0.x` phase, a breaking change is acceptable
  when it provides a meaningful improvement and has a sufficiently documented
  rationale; do not preserve an awkward API solely for compatibility.
- For public API renames or signature changes, update the implementation,
  tests, user documentation, and migration notes as one change. Keep the
  rationale explicit so future maintainers can distinguish intentional API
  evolution from accidental breakage.

## Testing

- Add or update tests when changing cancellation, context propagation, executor
  binding, or queue behavior.
- Run `mvn test` before finishing changes that touch the execution engine or
  public API.

## Ask Before

- Installing Maven dependencies or upgrading plugin versions.
- Deleting files or directories.
- Full release builds, PIT mutation tests, or `mvn deploy`.

Never commit secrets, `.env` files, GPG keys, or repository credentials.

## Subagent Usage

When the coding agent is Kimi Code, implement code changes directly in the
main agent; do not proactively delegate implementation to subagents. The only
exceptions are read-only exploration/analysis subagents and cases where the
user explicitly asks for subagent delegation.

## Reference Documents

- `docs/en/user-guide.md` - Read when changing user-facing behavior.
- `docs/en/migration-v0.2.md` - Breaking changes from the `0.1.x` API.

## Design Documents

Before changing the execution engine, cancellation, task groups, or queue
behavior, read `design/AGENTS.md` first and load only the documents whose
summaries match your task — do not pre-read everything.
Overview: task group contract → `design/task-group-*.md`; cancellation
propagation mechanics → `design/cancellation-propagation.md`; queue
lifecycle contract → `design/draining-queue-contract.md`; design rationale
and rejected ideas → `docs/zh/design/philosophy.md` and
`docs/zh/design/idea-graveyard.md`; immutable decision records → `adr/`.
