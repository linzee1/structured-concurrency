package io.github.monadrome.parallelinscope;

import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import javax.annotation.Nullable;

/**
 * A {@link ListenableFuture} for one task execution that also reports the task's name, its
 * deadline budget, and how it ended.
 *
 * <p>Every task execution future the library hands out implements this interface: the elements of
 * a {@code Par.map} batch, task-group members, the terminal combine, and the group completion
 * future. The interface is purely additive — it is still a {@code ListenableFuture}, so
 * {@code Futures.allAsList}, {@code addCallback}, and every other Guava combinator keep working
 * unchanged, and code that never checks for it behaves exactly as before. Use it through
 * {@code instanceof}:
 *
 * <pre>{@code
 * if (future instanceof TaskFuture) {
 *     TaskFuture<?> task = (TaskFuture<?>) future;
 *     if (task.outcome() == TaskOutcome.TIMEOUT) {
 *         log.warning(task.taskName() + " timed out with " + task.remaining() + " left");
 *     }
 * }
 * }</pre>
 *
 * <p>The library guarantees the interface is present, but not which class implements it: treat the
 * concrete implementation as private and never cast to it. Control handles that do not represent a
 * task execution — a batch's {@code submitCanceller}, for example — stay plain futures.
 *
 * <p>Attribution is per future. {@link #outcome()} reads the cancellation token that owns this one
 * task, so it is available while the enclosing group is still converging. The group's own terminal
 * classification — {@link TaskGroupResult#outcome()} and each member's {@link TaskCompletion} — is
 * derived after convergence and remains the authority for group-level attribution; it can carry a
 * finer cause (a fail-fast victim reads {@link TaskOutcome#FAIL_FAST} there) than the token chain
 * of a single future reveals.
 *
 * <p>Every method is safe on any thread at any point of the task's life: none of them block, throw,
 * or change the future. {@link #outcome()} and {@link #failure()} report a terminal value once the
 * future is done and never fall back to {@link TaskOutcome#RUNNING} afterwards. A terminal value
 * can still be refined while the enclosing scope settles: a group member learns about its group's
 * cancellation through the token chain, which may commit just after the member future was already
 * cancelled, so a caller that cancelled a member directly can read {@link
 * TaskOutcome#MEMBER_CANCELED} and then {@link TaskOutcome#GROUP_CANCELED} for the same future.
 * Reads taken once the enclosing scope has converged agree with each other.
 *
 * @param <T> the task result type
 */
public interface TaskFuture<T> extends ListenableFuture<T> {

    /** Returns the task name: the batch name, the declared member or combine name, or the group name. */
    String taskName();

    /**
     * Returns how this task ended, or {@link TaskOutcome#RUNNING} while it is still pending.
     *
     * <p>A failed task distinguishes {@link TaskOutcome#SUBMISSION_FAILURE} (it was rejected, or
     * otherwise failed before user code ran) from {@link TaskOutcome#USER_FAILURE}. A cancelled
     * task is attributed from its token chain, which yields {@link TaskOutcome#TIMEOUT}, {@link
     * TaskOutcome#FAIL_FAST}, {@link TaskOutcome#GROUP_CANCELED}, or — when no framework
     * cancellation path committed, meaning the caller cancelled the future directly — {@link
     * TaskOutcome#MEMBER_CANCELED}. A failure that merely reports observed cancellation (a
     * cooperative checkpoint, or an interrupt racing the cascade cancel) is attributed the same
     * way rather than as a user failure.
     */
    TaskOutcome outcome();

    /** Returns the absolute deadline in {@link System#nanoTime()} units, or {@link Long#MAX_VALUE} for none. */
    long deadlineNanos();

    /** Returns the remaining budget before {@link #deadlineNanos()}; never negative. */
    Duration remaining();

    /** Returns the failure behind a {@link TaskOutcome#USER_FAILURE} or {@code SUBMISSION_FAILURE}; null otherwise. */
    @Nullable
    Throwable failure();
}
