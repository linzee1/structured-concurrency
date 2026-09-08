package io.github.huatalk.parallelinscope.cancel;

import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Cooperative cancellation checkpoints and interruption-aware blocking operations.
 *
 * <p>Every public method checks the current scope's {@link CancellationToken} before starting its
 * operation. A canceled token produces a {@link LeanCancellationException}, except that {@link
 * #checkpoint(String, boolean)} can produce a standard {@link CancellationException} with a stack
 * trace when requested.
 *
 * <p>Blocking-operation adapters restore the interrupt flag and translate {@link
 * InterruptedException} into {@link LeanCancellationException}.
 *
 * <p>{@link #checkRunnable(Runnable, Class)} and {@link #checkSupplier(Supplier, Class)} instead
 * translate a matching failure into {@link CancellationException}, retaining the original failure
 * as its cause.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class Checkpoints {

    private Checkpoints() {}

    /**
     * Checks whether the named task has been canceled in the current scope.
     *
     * @param taskName the task expected in the current scope
     * @param lean whether to omit the cancellation stack trace
     * @throws LeanCancellationException if the matching task is canceled and {@code lean} is true
     * @throws CancellationException if the matching task is canceled and {@code lean} is false
     */
    public static void checkpoint(String taskName, boolean lean) {
        MultiTaskContext batch = currentBatchContext();
        if (batch != null) {
            if (taskName == null || !taskName.equals(batch.taskName())) return;
            checkCancellationToken(lean);
            return;
        }
        return;
    }

    /**
     * Checks the current cancellation token and the current thread's interrupt status. A token is not
     * required when this method is used as a raw interrupt checkpoint.
     *
     * @throws LeanCancellationException if the current scope is canceled or the thread is interrupted
     */
    public static void rawCheckpoint() {
        checkCancellationToken(true);
        if (Thread.interrupted()) {
            throw cancellation("Cancel during running by interruption");
        }
    }

    /**
     * Sleeps for the given number of milliseconds.
     *
     * @param millis sleep duration in milliseconds
     */
    public static void sleep(long millis) {
        checkCancellationToken(true);
        checkSleep(millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits until the latch reaches zero.
     *
     * @param latch the latch to await
     */
    public static void checkAwait(CountDownLatch latch) {
        checkCancellationToken(true);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw interrupted("Cancel during latch await by interruption", e);
        }
    }

    /**
     * Waits up to the given duration for the latch to reach zero.
     *
     * @param latch the latch to await
     * @param timeout the maximum time to wait
     * @return {@code true} if the latch reached zero, or {@code false} on timeout
     */
    public static boolean checkAwait(CountDownLatch latch, Duration timeout) {
        checkCancellationToken(true);
        return checkAwait(latch, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given timeout for the latch to reach zero.
     *
     * @param latch the latch to await
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code true} if the latch reached zero, or {@code false} on timeout
     */
    public static boolean checkAwait(CountDownLatch latch, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during latch await by interruption", e);
        }
    }

    /**
     * Waits up to the given duration for the condition to be signaled.
     *
     * @param condition the condition to await
     * @param timeout the maximum time to wait
     * @return {@code false} if the wait elapsed before being signaled; otherwise {@code true}
     */
    public static boolean checkAwait(Condition condition, Duration timeout) {
        checkCancellationToken(true);
        return checkAwait(condition, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given timeout for the condition to be signaled.
     *
     * @param condition the condition to await
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code false} if the wait elapsed before being signaled; otherwise {@code true}
     */
    public static boolean checkAwait(Condition condition, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            return condition.await(timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during condition await by interruption", e);
        }
    }

    /**
     * Waits for the thread to terminate.
     *
     * @param thread the thread whose termination to await
     */
    public static void checkJoin(Thread thread) {
        checkCancellationToken(true);
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw interrupted("Cancel during thread join by interruption", e);
        }
    }

    /**
     * Waits up to the given duration for the thread to terminate.
     *
     * @param thread the thread whose termination to await
     * @param timeout the maximum time to wait
     */
    public static void checkJoin(Thread thread, Duration timeout) {
        checkCancellationToken(true);
        checkJoin(thread, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given timeout for the thread to terminate.
     *
     * @param thread the thread whose termination to await
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     */
    public static void checkJoin(Thread thread, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            unit.timedJoin(thread, timeout);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during thread join by interruption", e);
        }
    }

    /**
     * Waits for and returns the future result.
     *
     * @param <V> the future result type
     * @param future the future to await
     * @return the completed future value
     * @throws ExecutionException if the future completed exceptionally
     */
    public static <V> V checkGet(Future<V> future) throws ExecutionException {
        checkCancellationToken(true);
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw interrupted("Cancel during future get by interruption", e);
        }
    }

    /**
     * Waits up to the given duration for the future result.
     *
     * @param <V> the future result type
     * @param future the future to await
     * @param timeout the maximum time to wait
     * @return the completed future value
     * @throws ExecutionException if the future completed exceptionally
     * @throws TimeoutException if the wait timed out
     */
    public static <V> V checkGet(Future<V> future, Duration timeout) throws ExecutionException, TimeoutException {
        checkCancellationToken(true);
        return checkGet(future, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given timeout for the future result.
     *
     * @param <V> the future result type
     * @param future the future to await
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return the completed future value
     * @throws ExecutionException if the future completed exceptionally
     * @throws TimeoutException if the wait timed out
     */
    public static <V> V checkGet(Future<V> future, long timeout, TimeUnit unit)
            throws ExecutionException, TimeoutException {
        checkCancellationToken(true);
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during future get by interruption", e);
        }
    }

    /**
     * Takes the head of the queue, waiting if necessary.
     *
     * @param <E> the queue element type
     * @param queue the queue from which to take an element
     * @return the head of the queue
     */
    public static <E> E checkTake(BlockingQueue<E> queue) {
        checkCancellationToken(true);
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw interrupted("Cancel during queue take by interruption", e);
        }
    }

    /**
     * Adds an element to the queue, waiting for capacity if necessary.
     *
     * @param <E> the queue element type
     * @param queue the queue to receive the element
     * @param element the element to enqueue
     */
    public static <E> void checkPut(BlockingQueue<E> queue, E element) {
        checkCancellationToken(true);
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during queue put by interruption", e);
        }
    }

    /**
     * Sleeps for the given duration.
     *
     * @param duration the duration to sleep
     */
    public static void checkSleep(Duration duration) {
        checkCancellationToken(true);
        checkSleep(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Sleeps for the given duration and unit.
     *
     * @param duration the duration to sleep
     * @param unit the unit of {@code duration}
     */
    public static void checkSleep(long duration, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during sleep by interruption", e);
        }
    }

    /**
     * Attempts to acquire one permit within the given duration.
     *
     * @param semaphore the semaphore from which to acquire
     * @param timeout the maximum time to wait
     * @return {@code true} if a permit was acquired, or {@code false} on timeout
     */
    public static boolean checkTryAcquire(Semaphore semaphore, Duration timeout) {
        checkCancellationToken(true);
        return checkTryAcquire(semaphore, 1, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Attempts to acquire one permit within the given timeout.
     *
     * @param semaphore the semaphore from which to acquire
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code true} if a permit was acquired, or {@code false} on timeout
     */
    public static boolean checkTryAcquire(Semaphore semaphore, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        return checkTryAcquire(semaphore, 1, timeout, unit);
    }

    /**
     * Attempts to acquire the permits within the given duration.
     *
     * @param semaphore the semaphore from which to acquire
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait
     * @return {@code true} if the permits were acquired, or {@code false} on timeout
     */
    public static boolean checkTryAcquire(Semaphore semaphore, int permits, Duration timeout) {
        checkCancellationToken(true);
        return checkTryAcquire(semaphore, permits, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Attempts to acquire the permits within the given timeout.
     *
     * @param semaphore the semaphore from which to acquire
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code true} if the permits were acquired, or {@code false} on timeout
     */
    public static boolean checkTryAcquire(Semaphore semaphore, int permits, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            return semaphore.tryAcquire(permits, timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during semaphore acquisition by interruption", e);
        }
    }

    /**
     * Attempts to acquire the lock within the given duration.
     *
     * @param lock the lock to acquire
     * @param timeout the maximum time to wait
     * @return {@code true} if the lock was acquired, or {@code false} on timeout
     */
    public static boolean checkTryLock(Lock lock, Duration timeout) {
        checkCancellationToken(true);
        return checkTryLock(lock, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Attempts to acquire the lock within the given timeout.
     *
     * @param lock the lock to acquire
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code true} if the lock was acquired, or {@code false} on timeout
     */
    public static boolean checkTryLock(Lock lock, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            return lock.tryLock(timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during lock acquisition by interruption", e);
        }
    }

    /**
     * Waits for the executor to terminate.
     *
     * @param executor the executor whose termination to await
     */
    public static void checkAwaitTermination(ExecutorService executor) {
        checkCancellationToken(true);
        checkAwaitTermination(executor, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given duration for the executor to terminate.
     *
     * @param executor the executor whose termination to await
     * @param timeout the maximum time to wait
     * @return {@code true} if the executor terminated, or {@code false} on timeout
     */
    public static boolean checkAwaitTermination(ExecutorService executor, Duration timeout) {
        checkCancellationToken(true);
        return checkAwaitTermination(executor, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits up to the given timeout for the executor to terminate.
     *
     * @param executor the executor whose termination to await
     * @param timeout the maximum time to wait
     * @param unit the unit of {@code timeout}
     * @return {@code true} if the executor terminated, or {@code false} on timeout
     */
    public static boolean checkAwaitTermination(ExecutorService executor, long timeout, TimeUnit unit) {
        checkCancellationToken(true);
        try {
            return executor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            throw interrupted("Cancel during executor termination wait by interruption", e);
        }
    }

    /**
     * Runs an action, translating a matching failure into a cancellation exception. Other unchecked
     * failures are propagated unchanged.
     *
     * @param <X> the exception type that triggers cancellation
     * @param action the action to execute
     * @param declaredType the exception class that triggers cancellation
     * @throws LeanCancellationException if the current scope is canceled before the action runs
     * @throws CancellationException if the action throws an instance of {@code declaredType}
     * @throws RuntimeException if the action throws a non-matching runtime exception
     * @throws Error if the action throws a non-matching error
     * @throws AssertionError if the action unexpectedly throws a checked throwable
     * @throws NullPointerException if {@code action} or {@code declaredType} is null
     */
    public static <X extends Throwable> void checkRunnable(Runnable action, Class<X> declaredType) {
        checkCancellationToken(true);
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(declaredType, "declaredType");
        try {
            action.run();
        } catch (Throwable t) {
            throwIfCancellationTrigger(t, declaredType, "checked action");
            rethrowUnchecked(t);
        }
    }

    /**
     * Gets a value, translating a matching failure into a cancellation exception. Other unchecked
     * failures are propagated unchanged.
     *
     * @param <T> the supplied value type
     * @param <X> the exception type that triggers cancellation
     * @param supplier the value supplier to execute
     * @param declaredType the exception class that triggers cancellation
     * @return the value produced by {@code supplier}
     * @throws LeanCancellationException if the current scope is canceled before the supplier runs
     * @throws CancellationException if the supplier throws an instance of {@code declaredType}
     * @throws RuntimeException if the supplier throws a non-matching runtime exception
     * @throws Error if the supplier throws a non-matching error
     * @throws AssertionError if the supplier unexpectedly throws a checked throwable
     * @throws NullPointerException if {@code supplier} or {@code declaredType} is null
     */
    public static <T, X extends Throwable> T checkSupplier(Supplier<? extends T> supplier, Class<X> declaredType) {
        checkCancellationToken(true);
        Objects.requireNonNull(supplier, "supplier");
        Objects.requireNonNull(declaredType, "declaredType");
        try {
            return supplier.get();
        } catch (Throwable t) {
            throwIfCancellationTrigger(t, declaredType, "checked supplier");
            return Checkpoints.rethrowUnchecked(t);
        }
    }

    /**
     * Propagates cancellation exceptions produced by this package. A cancellation exception supplied
     * as {@code ex} takes precedence over the current token's state.
     *
     * @param ex the exception to check
     * @throws CancellationException if {@code ex} is a cancellation exception or the current scope
     *     is canceled
     */
    public static void propagateCancellation(Throwable ex) {
        if (ex instanceof CancellationException) throw (CancellationException) ex;
        checkCancellationToken(true);
    }

    private static void checkCancellationToken(boolean lean) {
        MultiTaskContext batch = currentBatchContext();
        CancellationToken cancelToken = batch == null ? null : batch.cancellationToken();
        if (cancelToken != null && cancelToken.state().shouldInterruptCurrentThread()) {
            throw lean
                    ? new LeanCancellationException("Cancel during running")
                    : new CancellationException("Cancel during running");
        }
    }

    private static LeanCancellationException interrupted(String message, InterruptedException cause) {
        Thread.currentThread().interrupt();
        LeanCancellationException cancellation = cancellation(message);
        cancellation.initCause(cause);
        return cancellation;
    }

    private static MultiTaskContext currentBatchContext() {
        TaskExecutionContext currentTask = TaskExecutionContext.current();
        return currentTask == null ? null : currentTask.batchContext();
    }

    private static LeanCancellationException cancellation(String message) {
        return new LeanCancellationException(message);
    }

    private static <X extends Throwable> void throwIfCancellationTrigger(
            Throwable throwable, Class<X> declaredType, String operation) {
        if (declaredType.isInstance(throwable)) {
            X matched = declaredType.cast(throwable);
            CancellationException cancellation = new CancellationException(
                    "Cancel during " + operation + ": " + matched.getClass().getSimpleName());
            cancellation.initCause(matched);
            throw cancellation;
        }
    }

    /** Preserves unchecked failures while making an impossible checked failure explicit. */
    private static <T> T rethrowUnchecked(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new AssertionError("Runnable/Supplier threw a checked Throwable", throwable);
    }
}
