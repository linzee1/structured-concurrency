package io.github.huatalk.parallelinscope.scope;

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
    private final TaskGroupCompletionReason completionReason;
    private final @Nullable String failedMemberName;
    private final Map<String, TaskGroupMemberResult> members;

    TaskGroupResult(
            String groupId,
            String groupName,
            long startTimeNanos,
            long endTimeNanos,
            long deadlineNanos,
            TaskGroupCompletionReason completionReason,
            @Nullable String failedMemberName,
            Map<String, TaskGroupMemberResult> members) {
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.groupName = Objects.requireNonNull(groupName, "groupName cannot be null");
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.deadlineNanos = deadlineNanos;
        this.completionReason = Objects.requireNonNull(completionReason, "completionReason cannot be null");
        this.failedMemberName = failedMemberName;
        this.members = Collections.unmodifiableMap(new LinkedHashMap<>(members));
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

    public TaskGroupCompletionReason completionReason() {
        return completionReason;
    }

    public @Nullable String failedMemberName() {
        return failedMemberName;
    }

    public Map<String, TaskGroupMemberResult> members() {
        return members;
    }

    /** Returns the number of members admitted into this group. */
    public int memberCount() {
        return members.size();
    }
}
