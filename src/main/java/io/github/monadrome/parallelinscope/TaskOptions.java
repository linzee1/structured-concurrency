package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Immutable execution policy for exactly one task — a {@code Par.submit} task, a task-group
 * member, or a terminal combine.
 *
 * <p>This type carries only what one task execution reads: its deadline policy, its task type, and
 * its enqueue-rejection policy. Identity is not an option — a task's name is its {@link TaskKey},
 * or the explicit name passed to {@code Par.submit} — and fan-out is not an option either — a
 * single task has no parallelism to limit. A batch declares
 * {@link BatchOptions}; a coordination scope declares {@link TaskGroupOptions}.
 *
 * <p>The timeout is a forced explicit choice between two factories: {@link #inheritTimeout()}
 * declares that the enclosing scope's deadline is inherited, while {@link #timeout(Duration)} sets
 * an explicit one. No third state exists: omitting the choice does not compile, and the two
 * declarations cannot appear together.
 */
public final class TaskOptions {
    private static final TaskOptions INHERITED = new TaskOptions(null, TaskType.CPU_BOUND, true);

    private final @Nullable Duration timeout;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    private TaskOptions(@Nullable Duration timeout, TaskType taskType, boolean rejectEnqueue) {
        this.timeout = timeout;
        this.taskType = taskType;
        this.rejectEnqueue = rejectEnqueue;
    }

    /** Returns options whose deadline is inherited from the enclosing scope. */
    public static TaskOptions inheritTimeout() {
        return INHERITED;
    }

    /**
     * Returns options with an explicit timeout.
     *
     * @throws NullPointerException if {@code timeout} is null
     * @throws IllegalArgumentException if {@code timeout} is negative or zero
     */
    public static TaskOptions timeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout cannot be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive when configured");
        }
        return new TaskOptions(timeout, TaskType.CPU_BOUND, true);
    }

    /** Returns a copy of these options with the given task type. */
    public TaskOptions taskType(TaskType taskType) {
        return new TaskOptions(
                this.timeout, Objects.requireNonNull(taskType, "taskType cannot be null"), rejectEnqueue);
    }

    /**
     * Returns a copy of these options with the given enqueue-rejection policy.
     *
     * <p>The policy is honoured only when the registered executor's queue is a {@link
     * SmartBlockingQueue}; with any other queue this flag is inert.
     */
    public TaskOptions rejectEnqueue(boolean rejectEnqueue) {
        return new TaskOptions(timeout, taskType, rejectEnqueue);
    }

    /** The explicit execution timeout; empty means the enclosing scope's deadline is inherited. */
    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean rejectEnqueue() {
        return rejectEnqueue;
    }

    /** Adapts this policy to the kernel carrier of the task named {@code name}. */
    UnitSpec spec(String name) {
        return new UnitSpec(name, 1, Optional.ofNullable(timeout), taskType, rejectEnqueue);
    }
}
