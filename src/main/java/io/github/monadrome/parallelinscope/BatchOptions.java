package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Immutable options of one {@link Par#map} batch: its name, its requested parallelism, and the
 * execution policy applied to every element.
 *
 * <p>A batch is a fan-out over homogeneous input, so it owns exactly two concepts a single task
 * does not have: the batch identity shared by all elements and the concurrency limit of the sliding
 * window. Per-element execution policy is the rest of this type — timeout, task type, and enqueue
 * rejection.
 *
 * <p>The timeout is a forced explicit choice between {@link #inheritTimeout(String)} and {@link
 * #timeout(String, Duration)}; inheriting requires an enclosing scoped task at map time. Every
 * wither returns a new instance.
 */
public final class BatchOptions {
    private final String name;
    private final int parallelism;
    private final @Nullable Duration timeout;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    private BatchOptions(
            String name, int parallelism, @Nullable Duration timeout, TaskType taskType, boolean rejectEnqueue) {
        this.name = requireName(name);
        this.parallelism = parallelism;
        this.timeout = timeout;
        this.taskType = taskType;
        this.rejectEnqueue = rejectEnqueue;
    }

    /** Returns batch options that inherit the enclosing scope's deadline. */
    public static BatchOptions inheritTimeout(String name) {
        return new BatchOptions(name, -1, null, TaskType.CPU_BOUND, true);
    }

    /**
     * Returns batch options with an explicit timeout for the whole batch.
     *
     * @throws NullPointerException if {@code timeout} is null
     * @throws IllegalArgumentException if {@code timeout} is negative or zero
     */
    public static BatchOptions timeout(String name, Duration timeout) {
        return new BatchOptions(name, -1, requirePositive(timeout), TaskType.CPU_BOUND, true);
    }

    /** Returns a copy of these options with the given requested parallelism. */
    public BatchOptions parallelism(int parallelism) {
        return new BatchOptions(name, parallelism, timeout, taskType, rejectEnqueue);
    }

    /** Returns a copy of these options with the given task type. */
    public BatchOptions taskType(TaskType taskType) {
        return new BatchOptions(
                name, parallelism, timeout, Objects.requireNonNull(taskType, "taskType cannot be null"), rejectEnqueue);
    }

    /** Returns a copy of these options with the given enqueue-rejection policy. */
    public BatchOptions rejectEnqueue(boolean rejectEnqueue) {
        return new BatchOptions(name, parallelism, timeout, taskType, rejectEnqueue);
    }

    /** The batch name; every element of the batch shares it. */
    public String name() {
        return name;
    }

    /** Requested parallelism; non-positive means one worker per task. */
    public int parallelism() {
        return parallelism;
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

    /** Adapts these options to the kernel carrier of this batch. */
    UnitSpec spec() {
        return new UnitSpec(name, parallelism, Optional.ofNullable(timeout), taskType, rejectEnqueue);
    }

    private static String requireName(String name) {
        Objects.requireNonNull(name, "name cannot be null");
        if (name.trim().isEmpty()) throw new IllegalArgumentException("name cannot be empty");
        return name;
    }

    private static Duration requirePositive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout cannot be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive when configured");
        }
        return timeout;
    }
}
