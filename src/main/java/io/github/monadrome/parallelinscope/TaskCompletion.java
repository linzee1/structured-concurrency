package io.github.monadrome.parallelinscope;

import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Immutable record of one completed task — a {@code Par.map} batch element or a task-group
 * member.
 *
 * <p>The same record serves two delivery points: a {@code TaskListener} receives it at task
 * completion (carrying the task result), and a {@link TaskGroupResult} embeds one per member — plus
 * one for the optional terminal combine — as its terminal snapshot. Two fields are
 * delivery-specific: {@link #result()} is only non-null on
 * listener delivery of a successful task (a group member's result stays in its future), and
 * {@link #taskIndex()} is always zero for group members.
 *
 * <p>Listener events attribute the outcome observed at completion time — {@link
 * TaskOutcome#SUCCESS}, {@link TaskOutcome#USER_FAILURE}, or a cancellation state read from the
 * task token. A group snapshot may carry richer post-hoc attribution (for example {@link
 * TaskOutcome#FAIL_FAST}) derived after the group converges.
 *
 * <p>A member cancelled before running never marks a start or end time; its {@code
 * startTimeNanos} and {@code endTimeNanos} stay zero and the derived durations report zero.
 */
public final class TaskCompletion<T> {

    /** Queue wait beyond this threshold classifies the task as enqueued. */
    private static final long ENQUEUE_THRESHOLD_NANOS = 3_000_000L;

    private final String taskName;
    private final String unitId;
    private final int taskIndex;
    private final long submitTimeNanos;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final TaskOutcome outcome;
    private final @Nullable T result;
    private final @Nullable Throwable failure;

    private TaskCompletion(
            String taskName,
            String unitId,
            int taskIndex,
            long submitTimeNanos,
            long startTimeNanos,
            long endTimeNanos,
            TaskOutcome outcome,
            @Nullable T result,
            @Nullable Throwable failure) {
        this.taskName = Objects.requireNonNull(taskName, "taskName cannot be null");
        this.unitId = Objects.requireNonNull(unitId, "unitId cannot be null");
        if (taskIndex < 0) throw new IllegalArgumentException("taskIndex must not be negative");
        this.taskIndex = taskIndex;
        this.submitTimeNanos = submitTimeNanos;
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.outcome = Objects.requireNonNull(outcome, "outcome cannot be null");
        this.result = result;
        this.failure = failure;
    }

    /** Creates the record for a successful task, including a possibly-null result. */
    public static <T> TaskCompletion<T> succeeded(
            String taskName,
            String unitId,
            int taskIndex,
            long submitTimeNanos,
            long startTimeNanos,
            long endTimeNanos,
            @Nullable T result) {
        return new TaskCompletion<>(
                taskName,
                unitId,
                taskIndex,
                submitTimeNanos,
                startTimeNanos,
                endTimeNanos,
                TaskOutcome.SUCCESS,
                result,
                null);
    }

    /** Creates the record for a failed task; the outcome must be a non-success terminal state. */
    public static <T> TaskCompletion<T> failed(
            String taskName,
            String unitId,
            int taskIndex,
            long submitTimeNanos,
            long startTimeNanos,
            long endTimeNanos,
            TaskOutcome outcome,
            Throwable failure) {
        if (outcome == TaskOutcome.SUCCESS || outcome == TaskOutcome.RUNNING) {
            throw new IllegalArgumentException("failed record requires a non-success terminal outcome");
        }
        return new TaskCompletion<>(
                taskName,
                unitId,
                taskIndex,
                submitTimeNanos,
                startTimeNanos,
                endTimeNanos,
                outcome,
                null,
                Objects.requireNonNull(failure, "failure cannot be null"));
    }

    /**
     * Creates a group member's terminal snapshot: the registered member name stands in for {@code
     * taskName}, the index is zero, and the result stays in the member's future.
     */
    static TaskCompletion<Object> memberSnapshot(
            String memberName,
            String unitId,
            TaskOutcome outcome,
            @Nullable Throwable failure,
            long submitTimeNanos,
            long startTimeNanos,
            long endTimeNanos) {
        return new TaskCompletion<>(
                memberName, unitId, 0, submitTimeNanos, startTimeNanos, endTimeNanos, outcome, null, failure);
    }

    /**
     * Returns the logical task name: the unit name on listener delivery, the registered member
     * name in a group snapshot.
     */
    public String taskName() {
        return taskName;
    }

    /** Returns the stable identity of the multi-task unit invocation that owned this task. */
    public String unitId() {
        return unitId;
    }

    /** Returns the stable index of this task's input element within its unit; zero for group members. */
    public int taskIndex() {
        return taskIndex;
    }

    /** Returns the ticker reading at submission. */
    public long submitTimeNanos() {
        return submitTimeNanos;
    }

    /** Returns the ticker reading at execution start, or zero if the task never started. */
    public long startTimeNanos() {
        return startTimeNanos;
    }

    /** Returns the ticker reading at completion, or zero if the task never started. */
    public long endTimeNanos() {
        return endTimeNanos;
    }

    /** Returns how this task ended; never {@link TaskOutcome#RUNNING} in a delivered record. */
    public TaskOutcome outcome() {
        return outcome;
    }

    /** Returns whether the task completed successfully, including with a null result. */
    public boolean successful() {
        return outcome == TaskOutcome.SUCCESS;
    }

    /**
     * Returns the task result on listener delivery of a successful task; null for a failed task, a
     * successful null result, or any group snapshot.
     */
    public @Nullable T result() {
        return result;
    }

    /** Returns the task failure, or null on success. */
    public @Nullable Throwable failure() {
        return failure;
    }

    /**
     * Checks whether the task was classified as queued.
     *
     * @return {@code true} if the measured queue wait exceeded the threshold
     */
    public boolean enqueued() {
        return startTimeNanos - submitTimeNanos > ENQUEUE_THRESHOLD_NANOS;
    }

    /** Returns the execution duration, or zero if the task never started. */
    public Duration executionTime() {
        return Duration.ofNanos(Math.max(0L, endTimeNanos - startTimeNanos));
    }

    /** Returns the queue wait duration, or zero if the task never started. */
    public Duration waitTime() {
        return Duration.ofNanos(Math.max(0L, startTimeNanos - submitTimeNanos));
    }

    /** Returns the duration from submission to completion, or zero if the task never started. */
    public Duration totalTime() {
        return Duration.ofNanos(Math.max(0L, endTimeNanos - submitTimeNanos));
    }
}
