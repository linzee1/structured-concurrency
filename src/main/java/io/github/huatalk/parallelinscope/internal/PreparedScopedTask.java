package io.github.huatalk.parallelinscope.internal;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.BatchExecutionContext;
import io.github.huatalk.parallelinscope.scope.TaskType;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/** A prepared task whose future exists before the target executor is invoked. */
public final class PreparedScopedTask<V> {
    private final Executor executor;
    private final BatchExecutionContext batchContext;
    private final TaskType taskType;
    private final PreparedFuture<V> future;

    public PreparedScopedTask(
            Executor executor,
            BatchExecutionContext batchContext,
            TaskType taskType,
            Callable<V> callable,
            Consumer<? super ExecutionPhase> phaseObserver) {
        this.executor = Objects.requireNonNull(executor, "executor cannot be null");
        this.batchContext = Objects.requireNonNull(batchContext, "batchContext cannot be null");
        this.taskType = Objects.requireNonNull(taskType, "taskType cannot be null");
        this.future = new PreparedFuture<>(callable, phaseObserver);
    }

    public ListenableFuture<V> future() {
        return future;
    }

    /** Submits once; CPU-bound work uses the existing rejection-to-inline fallback. */
    public void submit() {
        BatchExecutionContext previous = SubmissionScope.install(batchContext);
        try {
            try {
                executor.execute(future);
            } catch (RejectedExecutionException rejected) {
                if (taskType == TaskType.CPU_BOUND) {
                    future.run();
                } else {
                    future.reject(rejected);
                }
            } catch (RuntimeException failure) {
                future.reject(failure);
            }
        } finally {
            SubmissionScope.restore(previous);
        }
    }

    /** Identifies a failure that occurred before user code started. */
    public static final class SubmissionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SubmissionException(Throwable cause) {
            super("Task submission failed", cause);
        }
    }

    private static final class PreparedFuture<V> extends AbstractFuture<V> implements Runnable {
        private static final Logger LOGGER = Logger.getLogger(PreparedFuture.class.getName());
        private static final Consumer<ExecutionPhase> NOOP = phase -> {};

        private final Callable<V> callable;
        private final AtomicReference<ExecutionPhase> phase = new AtomicReference<>(ExecutionPhase.SUBMITTED);
        private volatile Consumer<? super ExecutionPhase> observer;
        private volatile Thread runner;

        private PreparedFuture(Callable<V> callable, Consumer<? super ExecutionPhase> observer) {
            this.callable = Objects.requireNonNull(callable, "callable cannot be null");
            this.observer = Objects.requireNonNull(observer, "phaseObserver cannot be null");
        }

        @Override
        public void run() {
            if (!phase.compareAndSet(ExecutionPhase.SUBMITTED, ExecutionPhase.RUNNING)) return;
            runner = Thread.currentThread();
            notifyPhase(ExecutionPhase.RUNNING);
            try {
                if (!isCancelled()) set(callable.call());
            } catch (Throwable failure) {
                setException(failure);
            } finally {
                runner = null;
                phase.set(ExecutionPhase.TERMINAL);
                notifyPhase(ExecutionPhase.TERMINAL);
                observer = NOOP;
            }
        }

        private void reject(Throwable failure) {
            if (phase.compareAndSet(ExecutionPhase.SUBMITTED, ExecutionPhase.TERMINAL)) {
                setException(new SubmissionException(failure));
                notifyPhase(ExecutionPhase.TERMINAL);
                observer = NOOP;
            }
        }

        @Override
        protected void interruptTask() {
            Thread executing = runner;
            if (executing != null) executing.interrupt();
        }

        @Override
        protected void afterDone() {
            if (!isCancelled()) return;
            while (true) {
                ExecutionPhase current = phase.get();
                if (current == ExecutionPhase.SUBMITTED) {
                    if (phase.compareAndSet(current, ExecutionPhase.CANCELLED_BEFORE_RUN)) {
                        notifyPhase(ExecutionPhase.CANCELLED_BEFORE_RUN);
                        observer = NOOP;
                        return;
                    }
                } else if (current == ExecutionPhase.RUNNING) {
                    if (phase.compareAndSet(current, ExecutionPhase.CANCEL_REQUESTED_RUNNING)) {
                        notifyPhase(ExecutionPhase.CANCEL_REQUESTED_RUNNING);
                        return;
                    }
                } else {
                    return;
                }
            }
        }

        private void notifyPhase(ExecutionPhase value) {
            try {
                observer.accept(value);
            } catch (Throwable failure) {
                LOGGER.log(Level.WARNING, "Execution phase observer failed", failure);
            }
        }
    }
}
