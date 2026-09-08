package io.github.monadrome.parallelinscope.scope;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/** Immutable terminal snapshot for a parallel task group. */
public final class TaskGroupResult {
    private final String groupId;
    private final String groupName;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final long deadlineNanos;
    private final TaskOutcome outcome;
    private final @Nullable String failedMemberName;
    private final Map<String, TaskCompletion<?>> members;
    private final @Nullable TaskCompletion<?> terminal;

    TaskGroupResult(
            String groupId,
            String groupName,
            long startTimeNanos,
            long endTimeNanos,
            long deadlineNanos,
            TaskOutcome outcome,
            @Nullable String failedMemberName,
            Map<String, TaskCompletion<?>> members,
            @Nullable TaskCompletion<?> terminal) {
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.groupName = Objects.requireNonNull(groupName, "groupName cannot be null");
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.deadlineNanos = deadlineNanos;
        this.outcome = Objects.requireNonNull(outcome, "outcome cannot be null");
        this.failedMemberName = failedMemberName;
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

    public @Nullable String failedMemberName() {
        return failedMemberName;
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
}
