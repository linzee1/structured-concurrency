package io.github.monadrome.parallelinscope.internal;

import io.github.monadrome.parallelinscope.cancel.CancellationToken;
import io.github.monadrome.parallelinscope.scope.TaskOutcome;
import java.util.concurrent.CancellationException;

/**
 * Maps a {@link CancellationToken}'s committed state onto the {@link TaskOutcome} of the work it
 * canceled. This is the single implementation of the token-to-outcome attribution table shared by
 * {@code TaskGroup} member classification, {@code ScopedCallable} listener events, and {@code
 * TaskBatchResult} reports.
 *
 * <p>The mapping reads the token as the single authority, because a token commits its state before
 * canceling the futures bound to it:
 *
 * <ul>
 *   <li>{@code TIMEOUT} → {@link TaskOutcome#TIMEOUT}
 *   <li>{@code FAIL_FAST} → {@link TaskOutcome#FAIL_FAST}
 *   <li>{@code CANCELED} → {@link TaskOutcome#GROUP_CANCELED}: the token-as-a-whole was canceled,
 *       which is the post-hoc view of the owning unit. A caller doing direct observation of one
 *       task's own cancellation (see {@code ScopedCallable}) intercepts {@code CANCELED} first and
 *       reports {@link TaskOutcome#MEMBER_CANCELED} instead
 *   <li>{@code PROPAGATED_CANCELED} → the token carries no reason of its own, so {@link
 *       CancellationToken#originState()} walks the parent chain: an originating {@code TIMEOUT}
 *       stays {@link TaskOutcome#TIMEOUT}, anything else is {@link TaskOutcome#GROUP_CANCELED}
 *   <li>{@code RUNNING}/{@code SUCCESS} → no framework cancellation path committed; the caller
 *       supplies what an unattributed cancellation means in its context via {@code whenUncommitted}
 * </ul>
 */
public final class TokenOutcomes {

    private TokenOutcomes() {}

    /**
     * Attributes work canceled under {@code token} from the token's committed state.
     *
     * @param token the token owning the canceled work
     * @param whenUncommitted the outcome when the token is still {@code RUNNING} or {@code
     *     SUCCESS}, meaning no framework path canceled the work (for example a direct user
     *     cancellation)
     * @return the attributed outcome; never {@link TaskOutcome#RUNNING}
     */
    public static TaskOutcome forCanceled(CancellationToken token, TaskOutcome whenUncommitted) {
        switch (token.state()) {
            case TIMEOUT:
                return TaskOutcome.TIMEOUT;
            case FAIL_FAST:
                return TaskOutcome.FAIL_FAST;
            case PROPAGATED_CANCELED:
                return token.originState() == CancellationToken.State.TIMEOUT
                        ? TaskOutcome.TIMEOUT
                        : TaskOutcome.GROUP_CANCELED;
            case CANCELED:
                return TaskOutcome.GROUP_CANCELED;
            case SUCCESS:
            case RUNNING:
            default:
                return whenUncommitted;
        }
    }

    /**
     * Returns whether the failure merely signals that the task observed cancellation — a
     * cooperative checkpoint threw a {@link CancellationException}, or the worker thread was
     * interrupted — rather than a failure originating from user code. Such a failure can win the
     * race against the cascade {@code cancel(true)} on the task's future; the owning token, which
     * committed its state first, is then the correct attribution source.
     */
    public static boolean causedByCancellation(Throwable failure) {
        return failure instanceof CancellationException || failure instanceof InterruptedException;
    }
}
