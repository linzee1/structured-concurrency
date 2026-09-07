package io.github.huatalk.parallelinscope.scope;

import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nullable;

/** Immutable terminal snapshot for one group member. */
public final class TaskGroupMemberResult {
    private final String memberName;
    private final TaskOutcome outcome;
    private final @Nullable Throwable failure;
    private final long submitTimeNanos;
    private final long startTimeNanos;
    private final long endTimeNanos;

    TaskGroupMemberResult(
            String memberName,
            TaskOutcome outcome,
            @Nullable Throwable failure,
            long submitTimeNanos,
            long startTimeNanos,
            long endTimeNanos) {
        this.memberName = Objects.requireNonNull(memberName, "memberName cannot be null");
        this.outcome = Objects.requireNonNull(outcome, "outcome cannot be null");
        this.failure = failure;
        this.submitTimeNanos = submitTimeNanos;
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
    }

    public String memberName() {
        return memberName;
    }

    public TaskOutcome outcome() {
        return outcome;
    }

    public @Nullable Throwable failure() {
        return failure;
    }

    /**
     * Returns the ticker reading at submission.
     *
     * <p>A member cancelled before running never marks a start or end time; its {@code
     * startTimeNanos} and {@code endTimeNanos} stay zero.
     */
    public long submitTimeNanos() {
        return submitTimeNanos;
    }

    /** Returns the ticker reading at execution start, or zero if the member never started. */
    public long startTimeNanos() {
        return startTimeNanos;
    }

    /** Returns the ticker reading at completion, or zero if the member never started. */
    public long endTimeNanos() {
        return endTimeNanos;
    }

    /** Returns the execution duration, or zero if the member never started. */
    public Duration executionTime() {
        return Duration.ofNanos(Math.max(0L, endTimeNanos - startTimeNanos));
    }

    /** Returns the queue wait duration, or zero if the member never started. */
    public Duration waitTime() {
        return Duration.ofNanos(Math.max(0L, startTimeNanos - submitTimeNanos));
    }

    /** Returns the duration from submission to completion, or zero if the member never started. */
    public Duration totalTime() {
        return Duration.ofNanos(Math.max(0L, endTimeNanos - submitTimeNanos));
    }
}
