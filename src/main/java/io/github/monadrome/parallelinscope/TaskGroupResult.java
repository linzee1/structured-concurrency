package io.github.monadrome.parallelinscope;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

/** Immutable terminal snapshot for a parallel task group. */
public final class TaskGroupResult {
    private final String groupId;
    private final String groupName;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final long deadlineNanos;
    private final TaskOutcome outcome;
    private final @Nullable String failedTaskName;
    private final Map<String, TaskCompletion<?>> members;
    private final @Nullable TaskCompletion<?> terminal;

    TaskGroupResult(
            String groupId,
            String groupName,
            long startTimeNanos,
            long endTimeNanos,
            long deadlineNanos,
            TaskOutcome outcome,
            @Nullable String failedTaskName,
            Map<String, TaskCompletion<?>> members,
            @Nullable TaskCompletion<?> terminal) {
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.groupName = Objects.requireNonNull(groupName, "groupName cannot be null");
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.deadlineNanos = deadlineNanos;
        this.outcome = Objects.requireNonNull(outcome, "outcome cannot be null");
        this.failedTaskName = failedTaskName;
        this.members = Collections.unmodifiableMap(new LinkedHashMap<>(members));
        this.terminal = terminal;
    }

    public String groupId() {
        return groupId;
    }

    public String groupName() {
        return groupName;
    }

    public long startTimeNanos() {
        return startTimeNanos;
    }

    public long endTimeNanos() {
        return endTimeNanos;
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    /** Returns the terminal outcome of the group as a whole. */
    public TaskOutcome outcome() {
        return outcome;
    }

    /** Returns the key name of the first failed member or terminal combine, or null if none failed. */
    public @Nullable String failedTaskName() {
        return failedTaskName;
    }

    /** Returns each member's terminal snapshot, keyed by registered member name. */
    public Map<String, TaskCompletion<?>> members() {
        return members;
    }

    /**
     * Returns the terminal combine's snapshot, or null when the group declares no combine. A
     * combine cancelled before running never marks a start or end time, following the member
     * snapshot convention.
     */
    public @Nullable TaskCompletion<?> terminal() {
        return terminal;
    }

    /** Returns the number of members admitted into this group. */
    public int memberCount() {
        return members.size();
    }

    /**
     * Returns this result when the group succeeded; otherwise throws so a caller cannot forget the
     * failure. The outcome stays available as data through {@link #outcome()}; this is the loud
     * terminal accessor for call sites that have no use for a failed group's snapshot.
     *
     * <p>A recorded member or combine failure rethrows as-is when unchecked (checked failures are
     * wrapped in {@link CompletionException}); a cancellation-shaped outcome with no recorded
     * failure surfaces as {@link CancellationException} naming the outcome and the triggering task.
     *
     * @return this result, when the group succeeded
     * @throws CancellationException if the group was cancelled, timed out, or lost a member to
     *     cancellation
     * @throws RuntimeException the recorded failure, when a member or the combine failed
     * @throws Error the recorded failure, when a member or the combine threw an error
     */
    public TaskGroupResult orThrow() {
        if (outcome == TaskOutcome.SUCCESS) {
            return this;
        }
        TaskCompletion<?> failed = failedTaskSnapshot();
        Throwable failure = failed == null ? null : failed.failure();
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) {
            throw new CompletionException("Task group '" + groupName + "' failed in '" + failedTaskName + "'", failure);
        }
        throw new CancellationException("Task group '" + groupName + "' ended with " + outcome
                + (failedTaskName == null ? "" : " (triggered by '" + failedTaskName + "')"));
    }

    /**
     * Counts member outcomes — including the terminal combine when declared — symmetric with
     * {@link TaskBatchResult.BatchReport#stateCounts()}.
     *
     * @return the immutable outcome count map, empty for an empty group
     */
    public Map<TaskOutcome, Integer> outcomeCounts() {
        EnumMap<TaskOutcome, Integer> counts = new EnumMap<>(TaskOutcome.class);
        for (TaskCompletion<?> member : members.values()) {
            counts.merge(member.outcome(), 1, Integer::sum);
        }
        if (terminal != null) {
            counts.merge(terminal.outcome(), 1, Integer::sum);
        }
        return Collections.unmodifiableMap(counts);
    }

    /**
     * Returns a human-readable one-line summary, symmetric with {@link
     * TaskBatchResult#reportString()}.
     *
     * <p>Format: {@code STATE1:count,STATE2:count | outcome=OUTCOME, failedTask=name}
     *
     * @return formatted report string
     */
    public String reportString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<TaskOutcome, Integer> e : outcomeCounts().entrySet()) {
            if (!first) sb.append(',');
            sb.append(e.getKey()).append(':').append(e.getValue());
            first = false;
        }
        sb.append(" | outcome=").append(outcome);
        if (failedTaskName != null) {
            sb.append(", failedTask=").append(failedTaskName);
        }
        return sb.toString();
    }

    private @Nullable TaskCompletion<?> failedTaskSnapshot() {
        if (failedTaskName == null) {
            return null;
        }
        TaskCompletion<?> failed = members.get(failedTaskName);
        return failed != null ? failed : terminal;
    }
}
