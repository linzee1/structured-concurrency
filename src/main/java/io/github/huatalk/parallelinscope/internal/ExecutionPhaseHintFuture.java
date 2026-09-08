package io.github.huatalk.parallelinscope.internal;

import com.google.common.util.concurrent.AbstractFuture;
import io.github.huatalk.parallelinscope.scope.TaskOutcome;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A listenable future and runnable that publishes hints about its execution phase.
 *
 * <p>This is the shared single-task future for the whole library: both {@code Par.map} and
 * {@code TaskGroup} prepare it through {@link TaskSubmissions}, then the batch path
 * submits it via {@link ListenableCompletionService} while the group submits it after its frozen
 * build boundary. Both call sites share this one phase state machine; it must not be
 * re-implemented per caller.
 *
 * @param <V> result type
 */
public final class ExecutionPhaseHintFuture<V> extends AbstractFuture<V> implements RunnableFuture<V> {

    private static final Logger LOGGER = Logger.getLogger(ExecutionPhaseHintFuture.class.getName());
    private static final Consumer<ExecutionPhase> NOOP = phase -> {};

    /**
     * Tracks whether the worker or cancellation claimed the task first. The resulting phase is a hint
     * for consumers such as queue maintenance; it is not an exact queue-membership probe. This is
     * separate from the future state maintained by {@link AbstractFuture}.
     *
     * <pre>
     * Phase                       Meaning
     * --------------------------  -------------------------------------------------------------
     * SUBMITTED                   No worker has claimed the runnable yet.
     * RUNNING                     A worker has claimed the runnable.
     * CANCELED_BEFORE_RUN         Cancellation won before run() claimed the runnable.
     * CANCEL_REQUESTED_RUNNING    Cancellation followed a run() claim.
     * TERMINAL                    run() has returned.
     *
     * State transitions:
     *
     *                          cancellation wins
     * SUBMITTED ----------------------------------------------> CANCELED_BEFORE_RUN
     *     |
     *     | worker run() wins
     *     v
     *  RUNNING ---- cancellation succeeds ----> CANCEL_REQUESTED_RUNNING
     *     |                                              |
     *     +---------------- run() returns ---------------+
     *                            |
     *                            v
     *                         TERMINAL
     * </pre>
     */
    private final Callable<V> callable;

    private final AtomicReference<ExecutionPhase> phase = new AtomicReference<>(ExecutionPhase.SUBMITTED);

    private volatile Consumer<? super ExecutionPhase> phaseObserver;
    private volatile Thread runner;

    /** Creates a future with a phase observer. */
    public static <V> ExecutionPhaseHintFuture<V> create(
            Callable<V> callable, Consumer<? super ExecutionPhase> phaseObserver) {
        return new ExecutionPhaseHintFuture<>(callable, phaseObserver);
    }

    /** Creates a future with a phase observer for a runnable and fixed result. */
    public static <V> ExecutionPhaseHintFuture<V> create(
            Runnable runnable, V result, Consumer<? super ExecutionPhase> phaseObserver) {
        Objects.requireNonNull(runnable, "runnable cannot be null");
        return new ExecutionPhaseHintFuture<>(
                () -> {
                    runnable.run();
                    return result;
                },
                phaseObserver);
    }

    /** Wraps Guava's future semantics with task-local execution-phase hints. */
    private ExecutionPhaseHintFuture(Callable<V> callable, Consumer<? super ExecutionPhase> phaseObserver) {
        this.callable = Objects.requireNonNull(callable, "callable cannot be null");
        this.phaseObserver = Objects.requireNonNull(phaseObserver);
    }

    /**
     * Submits this deferred future to {@code executor} exactly once. A {@code CPU_BOUND} task that
     * the executor rejects runs inline; any other rejection, or a submission-time runtime failure,
     * fails the future with a {@link SubmissionException} without running user code.
     *
     * @param executor target executor
     * @param cpuBound whether the task may fall back to inline execution on rejection
     */
    public void submitPrepared(Executor executor, boolean cpuBound) {
        try {
            executor.execute(this);
        } catch (RejectedExecutionException rejected) {
            if (cpuBound) {
                run();
            } else {
                reject(rejected);
            }
        } catch (RuntimeException failure) {
            reject(failure);
        }
    }

    /**
     * Fails the future with a {@link SubmissionException} when it has not started running. A future
     * that already claimed {@code RUNNING} or is otherwise terminal is left untouched.
     */
    private void reject(Throwable failure) {
        if (phase.compareAndSet(ExecutionPhase.SUBMITTED, ExecutionPhase.TERMINAL)) {
            setException(new SubmissionException(failure));
            notifyPhase(ExecutionPhase.TERMINAL);
            phaseObserver = NOOP;
        }
    }

    /** Runs the delegate only after claiming the transition out of the submitted state. */
    @Override
    public void run() {
        if (!phase.compareAndSet(ExecutionPhase.SUBMITTED, ExecutionPhase.RUNNING)) {
            return;
        }
        runner = Thread.currentThread();
        notifyPhase(ExecutionPhase.RUNNING);
        boolean canceled = isCancelled();
        try {
            if (!canceled) {
                set(callable.call());
            }
        } catch (Throwable failure) {
            setException(failure);
        } finally {
            runner = null;
            // A cancel won mid-run if the runner saw it up front (skipped the call) or the
            // set()/setException() above lost the race (isCancelled() now true). Phase reads, CAS,
            // notification, and observer release are serialized with afterDone() under this monitor
            // so a cancel-phase emission can never be swallowed by an observer release racing it.
            synchronized (this) {
                boolean canceledNow = canceled || isCancelled();
                if (canceledNow
                        && phase.compareAndSet(ExecutionPhase.RUNNING, ExecutionPhase.CANCEL_REQUESTED_RUNNING)) {
                    notifyPhase(ExecutionPhase.CANCEL_REQUESTED_RUNNING);
                }
                // Advance to TERMINAL only if still in a running-phase state; never overwrite the
                // cancel phase just recorded above or by afterDone(). TERMINAL is always emitted
                // last, and the observer is released only here (or by afterDone() for
                // cancel-before-run).
                ExecutionPhase now = phase.get();
                if (now == ExecutionPhase.RUNNING || now == ExecutionPhase.CANCEL_REQUESTED_RUNNING) {
                    phase.set(ExecutionPhase.TERMINAL);
                }
                notifyPhase(ExecutionPhase.TERMINAL);
                phaseObserver = NOOP;
            }
        }
    }

    /** Classifies successful cancellation using the same state raced by {@link #run()}. */
    @Override
    protected void interruptTask() {
        Thread executing = runner;
        if (executing != null) {
            executing.interrupt();
        }
    }

    @Override
    protected void afterDone() {
        if (!isCancelled()) {
            return;
        }
        // Serialized with run()'s finally: the cancel-phase CAS and its notification are atomic with
        // respect to the runner's phase advance and observer release, so the cancel signal is either
        // emitted here (if this wins the phase CAS) or already emitted by the runner — never lost to
        // a racing observer release.
        synchronized (this) {
            ExecutionPhase current = phase.get();
            if (current == ExecutionPhase.SUBMITTED) {
                // Cancel won before run(): no worker will emit phases, so report it here and release.
                if (phase.compareAndSet(ExecutionPhase.SUBMITTED, ExecutionPhase.CANCELED_BEFORE_RUN)) {
                    notifyPhase(ExecutionPhase.CANCELED_BEFORE_RUN);
                    phaseObserver = NOOP;
                }
            } else if (current == ExecutionPhase.RUNNING) {
                // Cancel won while running: report it synchronously so the signal is visible as soon
                // as cancel() returns. The observer is NOT released here — run()'s finally emits
                // TERMINAL after this and performs the release.
                if (phase.compareAndSet(ExecutionPhase.RUNNING, ExecutionPhase.CANCEL_REQUESTED_RUNNING)) {
                    notifyPhase(ExecutionPhase.CANCEL_REQUESTED_RUNNING);
                }
            }
            // TERMINAL or CANCEL_REQUESTED_RUNNING: the runner already emitted (or is about to emit,
            // holding this same monitor) the cancel and terminal phases. Nothing further to do.
        }
    }

    private void notifyPhase(ExecutionPhase phase) {
        try {
            phaseObserver.accept(phase);
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Execution phase observer failed", e);
        }
    }

    /**
     * Classifies this future as a {@link TaskOutcome} using the richer phase hint, which separates
     * submission failure from user failure — a distinction a plain {@code Future} cannot express.
     * Cancellation always reads as {@link TaskOutcome#MEMBER_CANCELED} here; the finer TIMEOUT /
     * FAIL_FAST / GROUP_CANCELED reasons are written by the owning batch or group, not by this
     * future.
     */
    TaskOutcome outcome() {
        if (!isDone()) {
            return TaskOutcome.RUNNING;
        }
        if (isCancelled()) {
            return TaskOutcome.MEMBER_CANCELED;
        }
        try {
            get();
            return TaskOutcome.SUCCESS;
        } catch (java.util.concurrent.ExecutionException failure) {
            return failure.getCause() instanceof SubmissionException
                    ? TaskOutcome.SUBMISSION_FAILURE
                    : TaskOutcome.USER_FAILURE;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return TaskOutcome.USER_FAILURE;
        }
    }
}
