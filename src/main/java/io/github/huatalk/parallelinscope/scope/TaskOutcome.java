package io.github.huatalk.parallelinscope.scope;

/**
 * Unified running-or-terminal classification of a single task.
 *
 * <p>This is the single vocabulary used across the library for "how a task ended" (or whether it
 * is still running). It merges the former {@code FutureState} (batch report) and {@code
 * TaskGroupMemberReason} (group member result) enums: the terminal values are a strict refinement
 * of the old four-state future view, and {@link #RUNNING} absorbs the "not yet terminal" case.
 *
 * <p>{@link io.github.huatalk.parallelinscope.internal.FutureInspector} maps an arbitrary {@code
 * Future} onto these values conservatively; richer outcomes are available when the task exposes a
 * phase hint (see {@code ExecutionPhaseHintFuture}).
 *
 * <p>This enum is also the terminal vocabulary of a whole task group: {@link
 * TaskGroupResult#outcome()} reports one of {@link #SUCCESS}, {@link #USER_FAILURE}, {@link
 * #SUBMISSION_FAILURE}, {@link #TIMEOUT}, {@link #MEMBER_CANCELED}, or {@link #GROUP_CANCELED} —
 * a failing group adopts the failed member's own outcome. At group level, {@link #MEMBER_CANCELED}
 * means the cancellation originated from (or was applied directly to) a single member, while
 * {@link #GROUP_CANCELED} means the group was canceled as a whole or the cancellation propagated
 * down from an enclosing scope.
 */
public enum TaskOutcome {
    /** Task has not reached a terminal state. */
    RUNNING,
    /** User callable returned normally. */
    SUCCESS,
    /** User callable threw. */
    USER_FAILURE,
    /** Rejected or failed before user code ran. */
    SUBMISSION_FAILURE,
    /** Canceled directly by the caller. */
    MEMBER_CANCELED,
    /** Canceled because its owning group was canceled. */
    GROUP_CANCELED,
    /** Canceled as fail-fast fallout of a sibling failure. */
    FAIL_FAST,
    /** Canceled because a deadline was reached. */
    TIMEOUT;
}
