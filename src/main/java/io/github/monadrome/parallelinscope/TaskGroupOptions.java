package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Immutable options of one {@link TaskGroup} coordination scope: its name, its deadline, and its
 * convergence listeners.
 *
 * <p>A group is not a task execution, so this type declares no task type, no enqueue policy, and no
 * parallelism: those belong to each member's {@link TaskOptions}. The group-level timeout is a
 * forced explicit choice between {@link #inheritTimeout(String)} and {@link #timeout(String,
 * Duration)}; inheriting requires an enclosing scoped task at submit time.
 *
 * <p>Every wither returns a new instance, so options are safe to build once and reuse across
 * definitions and submissions.
 */
public final class TaskGroupOptions {
    private final String name;
    private final @Nullable Duration timeout;
    private final List<TaskGroupListener> listeners;

    private TaskGroupOptions(String name, @Nullable Duration timeout, List<TaskGroupListener> listeners) {
        this.name = requireName(name);
        this.timeout = timeout;
        this.listeners = listeners;
    }

    /** Returns group options that inherit the enclosing scope's deadline. */
    public static TaskGroupOptions inheritTimeout(String name) {
        return new TaskGroupOptions(name, null, Collections.emptyList());
    }

    /**
     * Returns group options with an explicit group deadline.
     *
     * @throws NullPointerException if {@code timeout} is null
     * @throws IllegalArgumentException if {@code timeout} is negative or zero
     */
    public static TaskGroupOptions timeout(String name, Duration timeout) {
        return new TaskGroupOptions(name, requirePositive(timeout), Collections.emptyList());
    }

    /** Returns a copy of these options with one more convergence listener. */
    public TaskGroupOptions listener(TaskGroupListener listener) {
        Objects.requireNonNull(listener, "listener cannot be null");
        List<TaskGroupListener> extended = new ArrayList<>(listeners);
        extended.add(listener);
        return new TaskGroupOptions(name, timeout, Collections.unmodifiableList(extended));
    }

    public String name() {
        return name;
    }

    /** The explicit group timeout; empty means the enclosing scope's deadline is inherited. */
    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    /** Immutable listener snapshot, copied at construction; {@code submit} uses this snapshot. */
    public List<TaskGroupListener> listeners() {
        return listeners;
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
