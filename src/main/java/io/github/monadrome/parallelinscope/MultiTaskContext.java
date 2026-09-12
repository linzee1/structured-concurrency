package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Immutable resolved state for one multi-task unit — a {@code Par.map} batch or one task-group
 * member; never cached by a {@code Par} or {@code GlobalPar}.
 *
 * <p>Resolution is the only place where a {@link UnitSpec} becomes executable values: requested
 * parallelism is capped by task count, an explicit timeout uses the earlier of its own and any
 * parent deadline, and an inherited timeout resolves to the enclosing deadline (rejected when there
 * is none). The cancellation token is always a new child token, so cancellation propagates downward
 * without making child failure cancel its parent.
 */
final class MultiTaskContext {
    /** Process-local unit identities: graph keys and diagnostics only, never persisted. */
    private static final AtomicLong UNIT_SEQUENCE = new AtomicLong();

    private final String unitId;
    private final String name;
    private final int taskCount;
    private final int effectiveParallelism;
    private final long deadlineNanos;
    private final CancellationToken cancellationToken;
    private final @Nullable MultiTaskContext structuralParent;
    private final @Nullable TaskGraphObservationScope taskGraphObservationScope;
    private final @Nullable ExecutorIdentity executorIdentity;
    private final @Nullable String executorLabel;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    private MultiTaskContext(
            String name,
            int taskCount,
            int effectiveParallelism,
            long deadlineNanos,
            CancellationToken cancellationToken,
            @Nullable MultiTaskContext structuralParent,
            @Nullable TaskGraphObservationScope taskGraphObservationScope,
            @Nullable ExecutorIdentity executorIdentity,
            @Nullable String executorLabel,
            TaskType taskType,
            boolean rejectEnqueue) {
        this.unitId = "unit-" + UNIT_SEQUENCE.incrementAndGet();
        this.name = name;
        this.taskCount = taskCount;
        this.effectiveParallelism = effectiveParallelism;
        this.deadlineNanos = deadlineNanos;
        this.cancellationToken = cancellationToken;
        this.structuralParent = structuralParent;
        this.taskGraphObservationScope = taskGraphObservationScope;
        this.executorIdentity = executorIdentity;
        this.executorLabel = executorLabel;
        this.taskType = taskType;
        this.rejectEnqueue = rejectEnqueue;
    }

    /**
     * Resolves a unit nested directly under an enclosing scoped task, without binding a concrete
     * {@code Par}.
     */
    static MultiTaskContext resolve(UnitSpec spec, int taskCount, @Nullable MultiTaskContext parent) {
        return resolve(spec, taskCount, parent, null);
    }

    static MultiTaskContext resolve(
            UnitSpec spec,
            int taskCount,
            @Nullable MultiTaskContext parent,
            @Nullable TaskGraphObservationScope taskGraphObservationScope) {
        return resolve(spec, taskCount, parent, taskGraphObservationScope, null, null);
    }

    /**
     * Resolves a unit while recording its concrete {@code Par} and supplied executor identity. The
     * identity is diagnostic and graph state, not a submission target; actual submission is owned by
     * the corresponding internal executor runtime.
     */
    static MultiTaskContext resolve(
            UnitSpec spec,
            int taskCount,
            @Nullable MultiTaskContext parent,
            @Nullable TaskGraphObservationScope taskGraphObservationScope,
            @Nullable ExecutorIdentity executorIdentity,
            @Nullable String parLabel) {
        Objects.requireNonNull(spec, "spec cannot be null");
        if (!spec.timeout().isPresent() && parent == null) {
            throw new IllegalArgumentException("no enclosing deadline to inherit; call timeout(Duration)");
        }
        TaskGraphObservationScope effectiveObservation = taskGraphObservationScope != null
                ? taskGraphObservationScope
                : parent == null ? null : parent.taskGraphObservationScope;
        return resolve(
                spec,
                taskCount,
                parent,
                parent == null ? null : parent.cancellationToken,
                parent == null ? Long.MAX_VALUE : parent.deadlineNanos,
                System.nanoTime(),
                effectiveObservation,
                executorIdentity,
                parLabel);
    }

    /**
     * Resolves a unit whose structural parent, cancellation parent, and deadline ceiling are
     * independent. This is used by task-group members, where group cancellation is not a graph
     * parent and the group deadline is not necessarily the structural parent's deadline.
     */
    static MultiTaskContext resolve(
            UnitSpec spec,
            int taskCount,
            @Nullable MultiTaskContext structuralParent,
            @Nullable CancellationToken cancellationParent,
            long deadlineCeilingNanos,
            long resolutionTimeNanos,
            @Nullable TaskGraphObservationScope taskGraphObservationScope,
            @Nullable ExecutorIdentity executorIdentity,
            @Nullable String parLabel) {
        Objects.requireNonNull(spec, "spec cannot be null");
        if (taskCount < 0) throw new IllegalArgumentException("taskCount must not be negative");
        int requested = spec.requestedParallelism();
        int effective = requested <= 0 ? taskCount : Math.min(requested, taskCount);
        long deadline = resolveDeadlineNanos(spec.timeout(), deadlineCeilingNanos, resolutionTimeNanos);
        return new MultiTaskContext(
                spec.name(),
                taskCount,
                effective,
                deadline,
                new CancellationToken(cancellationParent, deadline),
                structuralParent,
                taskGraphObservationScope,
                executorIdentity,
                parLabel,
                spec.taskType(),
                spec.rejectEnqueue());
    }

    /**
     * Resolves a deadline from an explicit timeout or an enclosing ceiling. An explicit timeout
     * expires at the earlier of its own deadline and the ceiling; an empty timeout inherits the
     * ceiling verbatim. Overflow saturates to {@link Long#MAX_VALUE}.
     */
    static long resolveDeadlineNanos(Optional<Duration> timeout, long ceilingNanos, long nowNanos) {
        if (!timeout.isPresent()) {
            return ceilingNanos;
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.get().toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long requestedDeadline = timeoutNanos > Long.MAX_VALUE - nowNanos ? Long.MAX_VALUE : nowNanos + timeoutNanos;
        return Math.min(requestedDeadline, ceilingNanos);
    }

    /** The logical unit name: batches use the options name; group members and combines use the key name. */
    public String name() {
        return name;
    }

    /** Stable identity for this one unit instance; never use name as graph identity. */
    public String unitId() {
        return unitId;
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

    /**
     * The structural parent used for nesting and graph edges. Cancellation parentage may differ:
     * a task-group member's cancellation parent is the group token, carried inside {@link
     * #cancellationToken()}, not by this field.
     */
    public @Nullable MultiTaskContext structuralParent() {
        return structuralParent;
    }

    public @Nullable TaskGraphObservationScope taskGraphObservationScope() {
        return taskGraphObservationScope;
    }

    public @Nullable ExecutorIdentity executorIdentity() {
        return executorIdentity;
    }

    /** Diagnostic label of the owning executor, or null when resolved without one. */
    public @Nullable String executorLabel() {
        return executorLabel;
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean rejectEnqueue() {
        return rejectEnqueue;
    }
}
