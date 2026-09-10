package io.github.huatalk.parallelinscope.internal;

import io.github.huatalk.parallelinscope.scope.TaskOutcome;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Java 8-compatible utility for inspecting {@link Future} state.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class FutureInspector {

    private FutureInspector() {}

    /**
     * Returns the current {@link TaskOutcome} of the given future.
     *
     * <p>Futures that carry a phase hint ({@link ExecutionPhaseHintFuture}) report a richer outcome
     * — distinguishing submission failure from user failure — because they track it themselves. Any
     * other {@code Future} exposes only done/cancelled state, so the mapping is conservative: a
     * failed future reads as {@link TaskOutcome#USER_FAILURE} and a cancelled one as {@link
     * TaskOutcome#MEMBER_CANCELED}.
     *
     * @param future the future to inspect
     * @return the current {@link TaskOutcome}
     */
    public static TaskOutcome state(Future<?> future) {
        if (future instanceof ExecutionPhaseHintFuture) {
            return ((ExecutionPhaseHintFuture<?>) future).outcome();
        }
        if (!future.isDone()) {
            return TaskOutcome.RUNNING;
        }
        if (future.isCancelled()) {
            return TaskOutcome.MEMBER_CANCELED;
        }
        try {
            future.get();
            return TaskOutcome.SUCCESS;
        } catch (ExecutionException e) {
            return TaskOutcome.USER_FAILURE;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskOutcome.USER_FAILURE;
        }
    }

    /**
     * Returns the exception from a failed future.
     *
     * @param future the future to inspect (must be done and failed)
     * @return the cause exception
     * @throws IllegalStateException if the future is not in a failed state
     */
    public static Throwable exceptionNow(Future<?> future) {
        if (!future.isDone()) {
            throw new IllegalStateException("Task has not completed");
        }
        if (future.isCancelled()) {
            throw new IllegalStateException("Task was canceled");
        }
        try {
            future.get();
            throw new IllegalStateException("Task completed with a result");
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while inspecting future", e);
        }
    }
}
