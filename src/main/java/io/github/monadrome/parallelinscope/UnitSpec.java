package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.Optional;

/**
 * Kernel-facing execution intent of one multi-task unit, adapted from a public option type.
 *
 * <p>A batch adapts {@link BatchOptions} — its own name and requested parallelism; a task-group
 * member or terminal combine adapts {@link TaskOptions} — the key's name and a requested
 * parallelism fixed at 1, because a single task has no fan-out to limit. The kernel resolves this
 * carrier and nothing else, so it never depends on which public entry point produced the values.
 *
 * <p>Package-private by design: adapting an option type to this carrier is the only seam between
 * the public option types and unit resolution, and it must not widen the public API.
 */
final class UnitSpec {
    private final String name;
    private final int requestedParallelism;
    private final Optional<Duration> timeout;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    UnitSpec(
            String name,
            int requestedParallelism,
            Optional<Duration> timeout,
            TaskType taskType,
            boolean rejectEnqueue) {
        this.name = name;
        this.requestedParallelism = requestedParallelism;
        this.timeout = timeout;
        this.taskType = taskType;
        this.rejectEnqueue = rejectEnqueue;
    }

    /** The logical unit name: a batch name for batches, the key name for group members. */
    String name() {
        return name;
    }

    /** Requested parallelism; non-positive means one worker per task. */
    int requestedParallelism() {
        return requestedParallelism;
    }

    /** The explicit timeout; empty means the enclosing scope's deadline is inherited. */
    Optional<Duration> timeout() {
        return timeout;
    }

    TaskType taskType() {
        return taskType;
    }

    boolean rejectEnqueue() {
        return rejectEnqueue;
    }
}
