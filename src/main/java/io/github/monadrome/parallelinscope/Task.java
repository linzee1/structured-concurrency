package io.github.monadrome.parallelinscope;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * The {@link TaskFuture} implementation the library delivers; instances are never constructed by
 * users.
 *
 * <p>A task is a thin forwarding shell over the future that actually carries its outcome, plus the
 * name and {@link CancellationToken} that belong to the task rather than to that future. The shell
 * owns its identity from creation on: a future waiting for a free concurrency slot already answers
 * with its final name, deadline, and token attribution before anything is submitted, and neither
 * submission nor abandonment changes them.
 *
 * <p>Forwarding is literal: {@link #get()}, {@link #cancel(boolean)}, {@link #isDone()}, and
 * {@link #addListener(Runnable, java.util.concurrent.Executor)} behave exactly as they do on the
 * delegate, including the interruption and cancellation-cascade semantics. Only read-only
 * information is added.
 *
 * <p>Two shapes exist, created by {@link #of} and {@link #placeholder}. A placeholder is handed to
 * the caller before its task has an executor slot; it stays registered with the caller's own
 * listeners and cancellations, and is later bound to the real future — or abandoned, when the task
 * will never run.
 */
public final class Task<T> extends ForwardingListenableFuture<T> implements TaskFuture<T> {

    private final ListenableFuture<T> delegate;
    private final String taskName;
    private final CancellationToken token;
    private final @Nullable SettableFuture<T> placeholder;
    private volatile boolean handedOff;

    private Task(
            String taskName,
            CancellationToken token,
            ListenableFuture<T> delegate,
            @Nullable SettableFuture<T> placeholder) {
        this.taskName = Objects.requireNonNull(taskName, "taskName cannot be null");
        this.token = Objects.requireNonNull(token, "token cannot be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
        this.placeholder = placeholder;
    }

    /**
     * Wraps a future that already represents this task's execution.
     *
     * @param <T> the task result type
     * @param taskName the task name reported by {@link #taskName()}
     * @param token the token owning this task's cancellation and deadline
     * @param delegate the future carrying the task's outcome
     * @return the task view over {@code delegate}
     */
    static <T> Task<T> of(String taskName, CancellationToken token, ListenableFuture<T> delegate) {
        return new Task<>(taskName, token, delegate, null);
    }

    /**
     * Creates a task whose execution is not submitted yet, backed by a fresh placeholder future.
     *
     * <p>A caller may register listeners and cancel the placeholder before it is bound; those
     * registrations stay on the placeholder, so {@link #bind} completes it by following the real
     * future instead of replacing the delegate.
     *
     * @param <T> the task result type
     * @param taskName the task name reported by {@link #taskName()}
     * @param token the token owning this task's cancellation and deadline
     * @return the task view over a new placeholder
     */
    static <T> Task<T> placeholder(String taskName, CancellationToken token) {
        SettableFuture<T> placeholder = SettableFuture.create();
        return new Task<>(taskName, token, placeholder, placeholder);
    }

    /**
     * Binds this placeholder to the future that will actually run the task.
     *
     * <p>From here on the placeholder follows {@code real}: the caller's listeners fire with its
     * outcome, and a placeholder cancelled beforehand cancels {@code real} in turn.
     *
     * @param real the submitted task future
     * @throws IllegalStateException if this task was not created as a placeholder
     */
    void bind(ListenableFuture<T> real) {
        SettableFuture<T> placeholder = placeholderOrThrow();
        handedOff = true;
        placeholder.setFuture(real);
    }

    /**
     * Completes a placeholder whose task will never be submitted, so the batch cannot stay pending.
     *
     * @param reason the failure reported to the caller, or {@code null} to complete the placeholder
     *     as cancelled when the batch itself is being cancelled
     * @throws IllegalStateException if this task was not created as a placeholder
     */
    void abandon(@Nullable Throwable reason) {
        SettableFuture<T> placeholder = placeholderOrThrow();
        handedOff = true;
        if (reason == null) {
            placeholder.cancel(true);
        } else {
            placeholder.setException(new SubmissionException(reason));
        }
    }

    private SettableFuture<T> placeholderOrThrow() {
        SettableFuture<T> current = placeholder;
        if (current == null) {
            throw new IllegalStateException("task '" + taskName + "' was not created as a placeholder");
        }
        return current;
    }

    @Override
    protected ListenableFuture<T> delegate() {
        return delegate;
    }

    @Override
    public String taskName() {
        return taskName;
    }

    @Override
    public TaskOutcome outcome() {
        if (!delegate.isDone()) {
            return TaskOutcome.RUNNING;
        }
        if (delegate.isCancelled()) {
            return TokenOutcomes.forCanceled(token, TaskOutcome.MEMBER_CANCELED);
        }
        try {
            // The delegate is done here, so read it with Futures.getDone: unlike get(), it never
            // throws InterruptedException, and a caller thread that happens to carry the
            // interrupt flag can no longer turn a success into a phantom USER_FAILURE.
            Futures.getDone(delegate);
            return TaskOutcome.SUCCESS;
        } catch (ExecutionException failure) {
            return classifyFailure(failure.getCause());
        }
    }

    /**
     * Classifies a completed failure. A failure that merely signals observed cancellation — a
     * checkpoint or an interrupt that won the race against the cascade cancel — is attributed
     * through the token instead of being recorded as a user failure.
     */
    private TaskOutcome classifyFailure(@Nullable Throwable cause) {
        if (cause instanceof SubmissionException) {
            return TaskOutcome.SUBMISSION_FAILURE;
        }
        if (TokenOutcomes.causedByCancellation(cause)) {
            return TokenOutcomes.forCanceled(token, TaskOutcome.USER_FAILURE);
        }
        return TaskOutcome.USER_FAILURE;
    }

    @Override
    public long deadlineNanos() {
        return token.deadlineNanos();
    }

    @Override
    public Duration remaining() {
        return token.remaining();
    }

    @Override
    public @Nullable Throwable failure() {
        TaskOutcome outcome = outcome();
        if (outcome != TaskOutcome.USER_FAILURE && outcome != TaskOutcome.SUBMISSION_FAILURE) {
            return null;
        }
        return FutureInspector.exceptionNow(delegate);
    }

    // ==================== package-private fluent derivation ====================

    /**
     * Returns a fluent view of the delegate; {@code from} returns an already-fluent future
     * untouched, so this adds nothing to delegates that are fluent already.
     */
    private FluentFuture<T> fluent() {
        return FluentFuture.from(delegate);
    }

    /** Wraps a future derived from this task, which keeps this task's name and token. */
    private <R> Task<R> derived(ListenableFuture<R> derived) {
        return new Task<>(taskName, token, derived, null);
    }

    /**
     * Mirrors {@link FluentFuture}'s chaining methods while keeping the result wrapped in a {@link
     * Task}, so internal composition cannot silently drop the task view. The seam is package-private
     * on purpose: a derived future is not a task the library executes, so it stays out of the
     * public contract ({@link TaskFuture} and {@code FluentFuture.from(task)} remain the caller's
     * route to fluent chaining), while the kernel keeps the name and the token that the chain
     * started from.
     */
    <R> Task<R> transform(Function<? super T, R> function, Executor executor) {
        return derived(fluent().transform(function, executor));
    }

    /** See {@link #transform(Function, Executor)}. */
    <R> Task<R> transformAsync(AsyncFunction<? super T, R> function, Executor executor) {
        return derived(fluent().transformAsync(function, executor));
    }

    /** See {@link #transform(Function, Executor)}. */
    <X extends Throwable> Task<T> catching(
            Class<X> exceptionType, Function<? super X, ? extends T> fallback, Executor executor) {
        return derived(fluent().catching(exceptionType, fallback, executor));
    }

    /** See {@link #transform(Function, Executor)}. */
    <X extends Throwable> Task<T> catchingAsync(
            Class<X> exceptionType, AsyncFunction<? super X, ? extends T> fallback, Executor executor) {
        return derived(fluent().catchingAsync(exceptionType, fallback, executor));
    }

    /**
     * See {@link #transform(Function, Executor)}. A future that only timeouts the chain is still
     * attributed through this task's token, which knows nothing about the derived deadline.
     */
    Task<T> withTimeout(Duration timeout, ScheduledExecutorService scheduledExecutor) {
        return derived(fluent().withTimeout(timeout, scheduledExecutor));
    }

    /** See {@link #withTimeout(Duration, ScheduledExecutorService)}. */
    Task<T> withTimeout(long timeout, TimeUnit unit, ScheduledExecutorService scheduledExecutor) {
        return derived(fluent().withTimeout(timeout, unit, scheduledExecutor));
    }

    /**
     * Returns a diagnostic string; task identity stays reference identity, so this is the only
     * non-forwarded {@link Object} method.
     */
    @Override
    public String toString() {
        StringBuilder description = new StringBuilder("Task[name=")
                .append(taskName)
                .append(", state=")
                .append(outcome())
                .append(", remaining=")
                .append(deadlineNanos() == Long.MAX_VALUE ? "unbounded" : remaining());
        if (placeholder != null) {
            description.append(", placeholder=").append(handedOff ? "handed-off" : "pending");
        }
        return description.append(']').toString();
    }
}
