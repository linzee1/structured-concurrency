package io.github.huatalk.parallelinscope.cancel;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.FAIL_FAST_CANCELED;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.MUTUAL_CANCELED;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.PROPAGATING_CANCELED;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.RUNNING;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.SUCCESS;
import static io.github.huatalk.parallelinscope.cancel.CancellationToken.State.TIMEOUT_CANCELED;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Cooperative cancellation token for parallel task groups.
 *
 * <p>A token owns a monotonic-clock deadline: it is created with no deadline, or as a child of a
 * parent token whose deadline is the ceiling. After task submission, {@link #bind(List,
 * ListenableFuture, ScheduledExecutorService)} connects the token to the submitted futures so a
 * reached deadline or a sibling failure triggers cancellation. Cancellation state is recorded by
 * the token before any cancel action runs, so observers can attribute a cancelled future to its
 * cause by reading {@link #state()}.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class CancellationToken {

    private static final long NO_DEADLINE_NANOS = Long.MAX_VALUE;

    private final SettableFuture<Object> futureToken = SettableFuture.create();
    private final AtomicReference<State> state = new AtomicReference<>(RUNNING);
    private final List<Consumer<State>> stateListeners = new CopyOnWriteArrayList<>();
    private final @Nullable CancellationToken parent;
    private final long deadlineNanos;

    private @Nullable volatile List<ListenableFuture<?>> boundFutures;
    private @Nullable volatile ListenableFuture<?> submitCanceller;

    /**
     * Creates a token linked to a parent, or a root token if {@code parent} is {@code null}.
     *
     * <p>The child inherits the parent's deadline ceiling: structured scopes may not outlive their
     * parent, so a child deadline is never later than the parent's.
     *
     * <p>This constructor is also the single parent-propagation mechanism: when the parent's work is
     * canceled, timed out, or fail-fast-canceled (any parent state for which interruption is
     * required), this token transitions to {@code PROPAGATING_CANCELED} and cancels its linked
     * work. No additional wiring in {@link #bind} or the caller is needed.
     *
     * @param parent the parent token, or {@code null} for a root token
     */
    public CancellationToken(@Nullable CancellationToken parent) {
        this(parent, parent == null ? NO_DEADLINE_NANOS : parent.deadlineNanos);
    }

    /**
     * Creates a token linked to a parent with an explicit deadline, or a root token with that
     * deadline when {@code parent} is {@code null}.
     *
     * <p>The effective deadline is {@code min(deadlineNanos, parent deadline)}: a child scope may
     * request an earlier deadline but never a later one.
     *
     * @param parent the parent token, or {@code null} for a root token
     * @param deadlineNanos the latest allowed completion instant on the monotonic clock
     */
    public CancellationToken(@Nullable CancellationToken parent, long deadlineNanos) {
        this.parent = parent;
        long parentCeiling = parent == null ? NO_DEADLINE_NANOS : parent.deadlineNanos;
        this.deadlineNanos = Math.min(deadlineNanos, parentCeiling);
        if (parent != null) {
            parent.futureToken.addListener(
                    () -> {
                        if (parent.state().shouldInterruptCurrentThread()) {
                            transitionTo(PROPAGATING_CANCELED);
                            futureToken.cancel(true);
                            cancelWork(true);
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
     * Returns this token's deadline on the monotonic clock, or {@link Long#MAX_VALUE} when it has
     * no deadline.
     *
     * @return the deadline instant in nanoseconds
     */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    /**
     * Returns the time remaining until the deadline.
     *
     * @return the remaining duration, or zero when the deadline has passed
     */
    public Duration remaining() {
        return Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime()));
    }

    /**
     * Connects this token to submitted work using the supplied timeout scheduler.
     *
     * <p>The deadline was fixed when the token was created; {@code bind} schedules the remaining
     * time with {@code timer}. If the deadline is already reached, this method synchronously takes
     * the timeout path instead of scheduling anything: it cancels the unfinished futures and the
     * submission future, while futures that already completed keep their results. A failure of any
     * bound future before the deadline cancels the remaining futures (fail-fast).
     *
     * @param <T> the task result type
     * @param futures the submitted task futures
     * @param submitCanceller the submission future to cancel with the tasks
     * @param timer scheduler used to detect the deadline
     */
    public <T> void bind(
            List<ListenableFuture<T>> futures, ListenableFuture<?> submitCanceller, ScheduledExecutorService timer) {
        Objects.requireNonNull(timer);
        List<ListenableFuture<?>> bound = new ArrayList<>(futures);
        this.boundFutures = bound;
        this.submitCanceller = submitCanceller;
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            transitionTo(TIMEOUT_CANCELED);
            futureToken.cancel(true);
            cancelWork(true);
            return;
        }
        ListenableFuture<?> listCanceller = Futures.allAsList(futures);
        FluentFuture<?> failFastFuture = FluentFuture.from(listCanceller)
                .withTimeout(Duration.ofNanos(remainingNanos), timer)
                .transform(ignored -> state.compareAndSet(RUNNING, SUCCESS), directExecutor())
                .catchingAsync(
                        Throwable.class,
                        ex -> {
                            transitionTo(ex instanceof TimeoutException ? TIMEOUT_CANCELED : FAIL_FAST_CANCELED);
                            cancelWork(true);
                            return Futures.immediateCancelledFuture();
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
        transitionTo(MUTUAL_CANCELED);
        futureToken.cancel(useInterrupt);
        cancelWork(useInterrupt);
    }

    /**
     * Cancels this token and its linked work because the deadline was reached.
     *
     * <p>This is the external trigger for callers that observe a deadline externally (for example a
     * task group escalating a member timeout to the group level) without waiting for this token's
     * own scheduled timeout.
     */
    public void timeoutCancel() {
        transitionTo(TIMEOUT_CANCELED);
        futureToken.cancel(true);
        cancelWork(true);
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
     * Registers a callback that runs synchronously when this token transitions out of {@code
     * RUNNING}.
     *
     * <p>The callback runs immediately after the state CAS and before any cancel action, so a
     * listener can observe and react to the exact terminal state. If the token is already terminal
     * when the listener is registered, the listener runs once with the current state. Intended for
     * {@code io.github.huatalk.parallelinscope.scope.ParallelTaskGroup}: escalating a member timeout
     * to the group token and canceling members through their tokens once the group state is fixed.
     * It is public only because the {@code scope} and {@code cancel} packages cannot share
     * package-private access.
     *
     * @param listener the state listener
     */
    public void addStateListener(Consumer<State> listener) {
        Objects.requireNonNull(listener, "listener cannot be null");
        State current = state.get();
        if (current != RUNNING) {
            listener.accept(current);
            return;
        }
        stateListeners.add(listener);
    }

    /**
     * Registers a callback that runs when this token is canceled or otherwise completes.
     *
     * <p>Intended for {@code io.github.huatalk.parallelinscope.scope.ParallelTaskGroup}: a group
     * linked to an outer batch's token listens so a parent cancellation fixes the group's first
     * completion reason without polling. It is public only because the {@code scope} and {@code
     * cancel} packages cannot share package-private access; it is not a general-purpose hook and
     * external callers should not rely on it.
     */
    public void addCompletionListener(Runnable listener, Executor executor) {
        futureToken.addListener(
                Objects.requireNonNull(listener, "listener cannot be null"),
                Objects.requireNonNull(executor, "executor cannot be null"));
    }

    private void transitionTo(State target) {
        if (state.compareAndSet(RUNNING, target)) {
            for (Consumer<State> listener : stateListeners) {
                listener.accept(target);
            }
        }
    }

    private void cancelWork(boolean useInterrupt) {
        ListenableFuture<?> canceller = submitCanceller;
        if (canceller != null) {
            canceller.cancel(useInterrupt);
        }
        List<ListenableFuture<?>> futures = boundFutures;
        if (futures != null) {
            for (ListenableFuture<?> future : futures) {
                future.cancel(useInterrupt);
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
        FAIL_FAST_CANCELED(-1),
        /** The task timed out. */
        TIMEOUT_CANCELED(-2),
        /** The token was explicitly canceled. */
        MUTUAL_CANCELED(-3),
        /** The parent token was canceled. */
        PROPAGATING_CANCELED(-4);

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
