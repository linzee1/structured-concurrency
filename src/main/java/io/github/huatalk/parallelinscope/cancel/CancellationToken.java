package io.github.huatalk.parallelinscope.cancel;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.CANCELED;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.FAIL_FAST;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.PROPAGATED_CANCELED;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.RUNNING;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.SUCCESS;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.TIMEOUT;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Cooperative cancellation token carrying a deadline for parallel work.
 *
 * <p>A token may be linked to a parent so that cancellation propagates to child task groups, and a
 * child never outlives its parent: the effective deadline is the minimum of the requested deadline
 * and the parent's. After task submission, {@link #bind(List, ListenableFuture,
 * ScheduledExecutorService)} connects the token to the submitted futures and enforces that
 * deadline together with fail-fast cancellation.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class CancellationToken {

    private static final Logger LOGGER = Logger.getLogger(CancellationToken.class.getName());

    private final SettableFuture<Object> futureToken = SettableFuture.create();
    private final AtomicReference<State> state = new AtomicReference<>(RUNNING);
    private final @Nullable CancellationToken parent;
    private final long deadlineNanos;
    private final List<Consumer<State>> stateListeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a token linked to a parent, or a root token if {@code parent} is {@code null}.
     *
     * <p>This constructor is the single parent-propagation mechanism: when the parent's work is
     * canceled, timed out, or fail-fast-canceled (any parent state for which interruption is
     * required), this token transitions to {@code PROPAGATED_CANCELED} and cancels its linked
     * future. No additional wiring in {@link #bind} or the caller is needed.
     *
     * @param parent the parent token, or {@code null} for a root token
     */
    public CancellationToken(@Nullable CancellationToken parent) {
        this(parent, Long.MAX_VALUE);
    }

    /**
     * Creates a token with a deadline on the monotonic clock, linked to a parent or unlinked.
     *
     * <p>The effective deadline is the minimum of {@code deadlineNanos} and the parent's deadline,
     * so a child scope can only request an earlier deadline, never a later one.
     *
     * @param parent the parent token, or {@code null} for a root token
     * @param deadlineNanos the requested deadline in {@link System#nanoTime()} units
     */
    public CancellationToken(@Nullable CancellationToken parent, long deadlineNanos) {
        this.parent = parent;
        this.deadlineNanos = parent == null ? deadlineNanos : Math.min(deadlineNanos, parent.deadlineNanos());
        if (parent != null) {
            parent.futureToken.addListener(
                    () -> {
                        if (parent.state().shouldInterruptCurrentThread() && transitionTo(PROPAGATED_CANCELED)) {
                            futureToken.cancel(true);
                        }
                    },
                    directExecutor());
        }
    }

    /** Creates an unlinked root token with no deadline. */
    public CancellationToken() {
        this(null);
    }

    /**
     * Creates an unlinked root token with no deadline.
     *
     * @return a new cancellation token
     */
    public static CancellationToken create() {
        return new CancellationToken();
    }

    /**
     * Returns the effective deadline in {@link System#nanoTime()} units; {@link Long#MAX_VALUE}
     * means no deadline.
     */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    /** Returns a non-negative remaining duration until this token's deadline. */
    public Duration remaining() {
        return Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime()));
    }

    /**
     * Connects this token to submitted work and arms its deadline.
     *
     * <p>After binding, the token classifies itself: {@code SUCCESS} when every future succeeds,
     * {@code TIMEOUT} when its deadline expires first, and {@code FAIL_FAST}
     * when any future fails. Every canceling transition cancels the futures and the submission
     * canceller; cancelling an already-successful future is a no-op, so a late cancel never
     * destroys a recorded result. An already-expired deadline simply schedules the timeout for
     * immediate execution.
     *
     * @param <T> the task result type
     * @param futures the submitted task futures
     * @param submitCanceller the submission future to cancel with the tasks
     * @param timer scheduler used to detect the deadline
     */
    public <T> void bind(
            List<ListenableFuture<T>> futures, ListenableFuture<?> submitCanceller, ScheduledExecutorService timer) {
        Objects.requireNonNull(timer);
        FluentFuture<?> failFastFuture = FluentFuture.from(Futures.allAsList(futures))
                .withTimeout(Duration.ofNanos(deadlineNanos - System.nanoTime()), timer);
        // A pending successfulAsList is the one cancellable handle that reaches both the task
        // futures and the submission canceller: it stays pending until every input is done, so
        // cancelling it still propagates after one task already failed or was cancelled.
        ListenableFuture<?> allFutures = Futures.successfulAsList(Futures.successfulAsList(futures), submitCanceller);
        failFastFuture.addCallback(
                new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        transitionTo(SUCCESS);
                    }

                    @Override
                    public void onFailure(Throwable failure) {
                        // Commit the state before cancelling: a listener can still fix a cause (a
                        // task group escalating a member timeout) before cascade cancellation makes
                        // every path look like fail-fast.
                        transitionTo(failure instanceof TimeoutException ? TIMEOUT : FAIL_FAST);
                        allFutures.cancel(true);
                    }
                },
                directExecutor());
        futureToken.setFuture(failFastFuture);
    }

    /**
     * Cancels this token and its linked work, interrupting running threads.
     *
     * <p>This is the common case; it is equivalent to {@link #cancel(boolean) cancel(true)}. Use
     * {@link #cancel(boolean)} with {@code false} only when running tasks must be allowed to
     * complete without interruption.
     */
    public void cancel() {
        cancel(true);
    }

    /**
     * Cancels this token and its linked work.
     *
     * @param useInterrupt whether to interrupt running threads
     */
    public void cancel(boolean useInterrupt) {
        if (transitionTo(CANCELED)) {
            futureToken.cancel(useInterrupt);
        }
    }

    /**
     * Cancels this token as a timeout without waiting for its own deadline to expire.
     *
     * <p>Intended for {@code io.github.huatalk.parallelinscope.scope.TaskGroup}: when a
     * member exceeds its own deadline, the group escalates that timeout onto the group token so
     * the group's completion reason stays {@code TIMEOUT} instead of collapsing into fail-fast.
     */
    public void timeoutCancel() {
        if (transitionTo(TIMEOUT)) {
            futureToken.cancel(true);
        }
    }

    /**
     * Returns the current state.
     *
     * @return the current state
     */
    public State state() {
        return state.get();
    }

    /**
     * Returns the terminal state that originated the cancellation, following propagation links.
     *
     * <p>When this token was canceled by its parent, its own state is {@code PROPAGATED_CANCELED}
     * and carries no reason; this method walks the parent chain to the first token whose terminal
     * state is not {@code PROPAGATED_CANCELED} and returns that originating state. A token that
     * reached its terminal state on its own (or is still running) simply reports {@link #state()}.
     * The walk is safe at any moment: a token only transitions to {@code PROPAGATED_CANCELED}
     * after its parent committed a terminal state, and terminal states never change afterwards.
     *
     * @return the originating terminal state, or the current state when nothing propagated
     */
    public State originState() {
        CancellationToken token = this;
        State s = token.state();
        while (s == PROPAGATED_CANCELED && token.parent != null) {
            token = token.parent;
            s = token.state();
        }
        return s;
    }

    /**
     * Registers a callback invoked synchronously right after a terminal transition commits and
     * before the associated cancellation actions run.
     *
     * <p>Intended for {@code io.github.huatalk.parallelinscope.scope.TaskGroup}: a group
     * listens on a member token so a member timeout escalates to the group before cascade
     * cancellation runs. It is public only because the {@code scope} and {@code cancel} packages
     * cannot share package-private access; it is not a general-purpose hook and external callers
     * should not rely on it. A listener registered after the token left {@code RUNNING} is never
     * invoked. Listener failures are logged and swallowed; they must not break cancellation.
     */
    public void addStateListener(Consumer<State> listener) {
        stateListeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
    }

    /** Commits a terminal transition from {@code RUNNING}, notifying state listeners when it wins. */
    private boolean transitionTo(State terminal) {
        if (state.compareAndSet(RUNNING, terminal)) {
            notifyStateListeners(terminal);
            return true;
        }
        return false;
    }

    private void notifyStateListeners(State newState) {
        for (Consumer<State> listener : stateListeners) {
            try {
                listener.accept(newState);
            } catch (Throwable failure) {
                LOGGER.log(Level.WARNING, "CancellationToken state listener failed", failure);
            }
        }
    }

    /** Lifecycle state of a {@link CancellationToken}. */
    public enum State {

        /** The task is running. */
        RUNNING(0),
        /** The task completed successfully. */
        SUCCESS(1),
        /** A sibling task failed, triggering fail-fast cancellation. */
        FAIL_FAST(-1),
        /** The task timed out. */
        TIMEOUT(-2),
        /** The token was explicitly canceled. */
        CANCELED(-3),
        /** The parent token was canceled. */
        PROPAGATED_CANCELED(-4);

        private final int code;

        State(int code) {
            this.code = code;
        }

        /** Returns the state code. */
        public int code() {
            return code;
        }

        /** Returns whether this state requires interruption. */
        boolean shouldInterruptCurrentThread() {
            return code < 0;
        }
    }
}
