package io.github.huatalk.parallelinscope.scope;

import java.util.Objects;
import javax.annotation.Nullable;

/** Immutable terminal snapshot for one group member. */
public final class TaskGroupMemberResult {
    private final String memberName;
    private final TaskOutcome completionReason;
    private final @Nullable Throwable failure;
    private final TaskContext taskContext;

    TaskGroupMemberResult(
            String memberName, TaskOutcome completionReason, @Nullable Throwable failure, TaskContext taskContext) {
        this.memberName = Objects.requireNonNull(memberName, "memberName cannot be null");
        this.completionReason = Objects.requireNonNull(completionReason, "completionReason cannot be null");
        this.failure = failure;
        this.taskContext = Objects.requireNonNull(taskContext, "taskContext cannot be null");
    }

    public String memberName() {
        return memberName;
    }

    public TaskOutcome completionReason() {
        return completionReason;
    }

    public @Nullable Throwable failure() {
        return failure;
    }

    public TaskContext taskContext() {
        return taskContext;
    }
}
