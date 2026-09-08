package io.github.huatalk.parallelinscope.internal;

import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.TaskContext;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Per-task state for one element of a batch. */
public final class TaskExecutionContext implements TaskContext {

    private static final ThreadLocal<TaskExecutionContext> CURRENT = new ThreadLocal<>();

    private final MultiTaskContext batchContext;
    private final int taskIndex;
    private final long submitTimeNanos;

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    public TaskExecutionContext(MultiTaskContext batchContext, int taskIndex, long submitTimeNanos) {
        this.batchContext = Objects.requireNonNull(batchContext, "batchContext cannot be null");
        if (taskIndex < 0) throw new IllegalArgumentException("taskIndex must not be negative");
        this.taskIndex = taskIndex;
        this.submitTimeNanos = submitTimeNanos;
    }

    public MultiTaskContext batchContext() {
        return batchContext;
    }

    /** Returns the stable index of this task's input element within its batch. */
    public int taskIndex() {
        return taskIndex;
    }

    public long submitTimeNanos() {
        return submitTimeNanos;
    }

    public long startTimeNanos() {
        return startTimeNanos;
    }

    public long endTimeNanos() {
        return endTimeNanos;
    }

    public long executionTimeNanos() {
        return endTimeNanos - startTimeNanos;
    }

    public long waitTimeNanos() {
        return startTimeNanos - submitTimeNanos;
    }

    public long totalTimeNanos() {
        return endTimeNanos - submitTimeNanos;
    }

    /** Returns the task currently executing on this thread, or null outside a scoped task. */
    public static @Nullable TaskExecutionContext current() {
        return CURRENT.get();
    }

    /** Installs this task as current and returns the task it replaced. */
    static @Nullable TaskExecutionContext install(TaskExecutionContext context) {
        TaskExecutionContext previous = CURRENT.get();
        CURRENT.set(context);
        return previous;
    }

    /** Restores a task previously returned from {@link #install(TaskExecutionContext)}. */
    static void restore(@Nullable TaskExecutionContext context) {
        if (context == null) CURRENT.remove();
        else CURRENT.set(context);
    }

    void markStarted(long timeNanos) {
        startTimeNanos = timeNanos;
    }

    void markEnded(long timeNanos) {
        endTimeNanos = timeNanos;
    }
}
