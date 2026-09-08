package io.github.huatalk.parallelinscope.scope;

import io.github.huatalk.parallelinscope.cancel.CancellationToken;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Immutable resolved state for one {@code Par.map} invocation; never cached by a {@code Par} or
 * {@code GlobalPar}.
 *
 * <p>Resolution is the only place where user options become executable values: requested
 * parallelism is capped by task count, absent timeout uses the global default, and a nested batch
 * uses the earlier of its requested and parent deadlines. The cancellation token is always a new
 * child token, so cancellation propagates downward without making child failure cancel its parent.
 */
public final class BatchExecutionContext {
    private final String batchId;
    private final String taskName;
    private final int taskCount;
    private final int effectiveParallelism;
    private final long deadlineNanos;
    private final CancellationToken cancellationToken;
    private final BatchExecutionContext parent;
    private final TaskGraphObservationContext taskGraphObservationContext;
    private final ExecutorIdentity executorIdentity;
    private final String parLabel;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    private BatchExecutionContext(
            String taskName,
            int taskCount,
            int effectiveParallelism,
            long deadlineNanos,
            CancellationToken cancellationToken,
            BatchExecutionContext parent,
            TaskGraphObservationContext taskGraphObservationContext,
            ExecutorIdentity executorIdentity,
            String parLabel,
            TaskType taskType,
            boolean rejectEnqueue) {
        this.batchId = UUID.randomUUID().toString();
        this.taskName = taskName;
        this.taskCount = taskCount;
        this.effectiveParallelism = effectiveParallelism;
        this.deadlineNanos = deadlineNanos;
        this.cancellationToken = cancellationToken;
        this.parent = parent;
        this.taskGraphObservationContext = taskGraphObservationContext;
        this.executorIdentity = executorIdentity;
        this.parLabel = parLabel;
        this.taskType = taskType;
        this.rejectEnqueue = rejectEnqueue;
    }

    /**
     * Resolves public options without binding a concrete {@code Par}. This overload is intended for
     * compatibility and tests; normal execution uses the identity-aware overload below.
     */
    public static BatchExecutionContext resolve(
            GlobalExecutionPolicy policy,
            BatchExecutionOptions options,
            int taskCount,
            @Nullable BatchExecutionContext parent) {
        return resolve(policy, options, taskCount, parent, null);
    }

    public static BatchExecutionContext resolve(
            GlobalExecutionPolicy policy,
            BatchExecutionOptions options,
            int taskCount,
            @Nullable BatchExecutionContext parent,
            @Nullable TaskGraphObservationContext taskGraphObservationContext) {
        Objects.requireNonNull(policy);
        Objects.requireNonNull(options);
        if (taskCount < 0) throw new IllegalArgumentException("taskCount must not be negative");
        int requested = options.parallelism();
        int effective = requested <= 0 ? taskCount : Math.min(requested, taskCount);
        long timeoutMillis;
        if (options.timeout() == null) {
            timeoutMillis = policy.defaultTimeoutMillis();
        } else {
            try {
                timeoutMillis = options.timeout().toMillis();
            } catch (ArithmeticException overflow) {
                timeoutMillis = Long.MAX_VALUE / 1_000_000L;
            }
        }
        long now = System.nanoTime();
        long timeoutNanos;
        try {
            timeoutNanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long requestedDeadline = timeoutNanos > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + timeoutNanos;
        long deadline = parent == null ? requestedDeadline : Math.min(parent.deadlineNanos, requestedDeadline);
        CancellationToken token = new CancellationToken(parent == null ? null : parent.cancellationToken, deadline);
        TaskGraphObservationContext effectiveObservation = taskGraphObservationContext != null
                ? taskGraphObservationContext
                : parent == null ? null : parent.taskGraphObservationContext;
        return new BatchExecutionContext(
                options.taskName(),
                taskCount,
                effective,
                deadline,
                token,
                parent,
                effectiveObservation,
                null,
                null,
                options.taskType(),
                options.rejectEnqueue());
    }

    /**
     * Resolves a batch while recording its concrete {@code Par} and supplied executor identity. The
     * identity is diagnostic and graph state, not a submission target; actual submission is owned by
     * the corresponding internal executor runtime.
     */
    public static BatchExecutionContext resolve(
            GlobalExecutionPolicy policy,
            BatchExecutionOptions options,
            int taskCount,
            @Nullable BatchExecutionContext parent,
            @Nullable TaskGraphObservationContext taskGraphObservationContext,
            ExecutorIdentity executorIdentity,
            String parLabel) {
        BatchExecutionContext context = resolve(policy, options, taskCount, parent, taskGraphObservationContext);
        return new BatchExecutionContext(
                context.taskName,
                context.taskCount,
                context.effectiveParallelism,
                context.deadlineNanos,
                context.cancellationToken,
                context.parent,
                context.taskGraphObservationContext,
                executorIdentity,
                parLabel,
                context.taskType,
                context.rejectEnqueue);
    }

    /**
     * Resolves a batch whose structural parent, cancellation parent, and deadline ceiling are
     * independent. This is used by task-group members, where group cancellation is not a graph
     * parent and the group deadline is not necessarily the structural parent's deadline.
     */
    static BatchExecutionContext resolve(
            GlobalExecutionPolicy policy,
            BatchExecutionOptions options,
            int taskCount,
            @Nullable BatchExecutionContext structuralParent,
            @Nullable CancellationToken cancellationParent,
            long deadlineCeilingNanos,
            long resolutionTimeNanos,
            @Nullable TaskGraphObservationContext taskGraphObservationContext,
            ExecutorIdentity executorIdentity,
            String parLabel) {
        Objects.requireNonNull(policy, "policy cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        if (taskCount < 0) throw new IllegalArgumentException("taskCount must not be negative");
        int requested = options.parallelism();
        int effective = requested <= 0 ? taskCount : Math.min(requested, taskCount);
        long timeoutMillis;
        if (options.timeout() == null) {
            timeoutMillis = policy.defaultTimeoutMillis();
        } else {
            try {
                timeoutMillis = options.timeout().toMillis();
            } catch (ArithmeticException overflow) {
                timeoutMillis = Long.MAX_VALUE / 1_000_000L;
            }
        }
        long timeoutNanos;
        try {
            timeoutNanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long requestedDeadline = timeoutNanos > Long.MAX_VALUE - resolutionTimeNanos
                ? Long.MAX_VALUE
                : resolutionTimeNanos + timeoutNanos;
        long deadline = Math.min(requestedDeadline, deadlineCeilingNanos);
        return new BatchExecutionContext(
                options.taskName(),
                taskCount,
                effective,
                deadline,
                new CancellationToken(cancellationParent, deadline),
                structuralParent,
                taskGraphObservationContext,
                executorIdentity,
                parLabel,
                options.taskType(),
                options.rejectEnqueue());
    }

    public String taskName() {
        return taskName;
    }

    /** Stable identity for this one batch instance; never use taskName as graph identity. */
    public String batchId() {
        return batchId;
    }

    public int taskCount() {
        return taskCount;
    }

    public int effectiveParallelism() {
        return effectiveParallelism;
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    /** Returns a non-negative remaining timeout derived from the monotonic clock. */
    public Duration remaining() {
        long nanos = Math.max(0L, deadlineNanos - System.nanoTime());
        return Duration.ofNanos(nanos);
    }

    public CancellationToken cancellationToken() {
        return cancellationToken;
    }

    public BatchExecutionContext parent() {
        return parent;
    }

    public TaskGraphObservationContext taskGraphObservationContext() {
        return taskGraphObservationContext;
    }

    public ExecutorIdentity executorIdentity() {
        return executorIdentity;
    }

    public String parLabel() {
        return parLabel;
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean rejectEnqueue() {
        return rejectEnqueue;
    }
}
