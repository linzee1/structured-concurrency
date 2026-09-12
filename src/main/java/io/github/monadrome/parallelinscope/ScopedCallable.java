package io.github.monadrome.parallelinscope;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central task wrapper with full lifecycle instrumentation.
 *
 * <p>Wraps a {@link Callable} with:
 *
 * <ul>
 *   <li>Context setup (TaskExecutionContext)
 *   <li>Cooperative cancellation checkpoint
 *   <li>Timing metrics via SPI {@link TaskListener} callbacks
 *   <li>Cleanup on completion
 * </ul>
 *
 * <p>The active {@link TaskExecutionContext} is available to inner callables through {@link
 * TaskExecutionContext#current()}.
 *
 * <p>Timeline: {@code submitTime -> startTime -> endTime}
 *
 * @param <V> return value type
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
final class ScopedCallable<V> implements Callable<V> {

    private static final Logger logger = Logger.getLogger(ScopedCallable.class.getName());

    private final Callable<V> delegate;
    private final com.google.common.base.Ticker ticker;
    private final TaskExecutionContext taskContext;
    private final List<TaskListener> taskListeners;

    /** Creates a task wrapper from the batch context owned by one GlobalPar execution. */
    ScopedCallable(TaskExecutionContext taskContext, Callable<V> delegate, List<TaskListener> taskListeners) {
        this.taskContext = Objects.requireNonNull(taskContext, "taskContext cannot be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
        this.ticker = com.google.common.base.Ticker.systemTicker();
        this.taskListeners = taskListeners == null ? java.util.Collections.emptyList() : taskListeners;
    }

    @Override
    public V call() throws Exception {
        // ==================== prepareContext ====================
        TaskExecutionContext previousTask = TaskExecutionContext.install(taskContext);

        // TaskGraphObservationScope is a TransmittableThreadLocal captured by the TtlCallable
        // wrapper created at the Par.map boundary.

        V result = null;
        Throwable taskException = null;
        try {
            // ==================== doCall ====================
            taskContext.markStarted(ticker.read());
            Checkpoints.checkpoint();
            result = delegate.call();
            return result;
        } catch (Throwable t) {
            taskException = t;
            throw t;
        } finally {
            // ==================== cleanup & metrics ====================
            taskContext.markEnded(ticker.read());
            // Listeners receive the completed task explicitly and never inherit an implicit
            // current-task identity, even when this callable ran inline inside another task.
            TaskExecutionContext.restore(null);
            try {
                notifyListeners(result, taskException);
            } finally {
                TaskExecutionContext.restore(previousTask);
            }
        }
    }

    private void notifyListeners(V result, Throwable exception) {
        List<TaskListener> listeners = taskListeners;
        if (listeners.isEmpty()) {
            return;
        }
        MultiTaskContext unit = taskContext.multiTaskContext();
        String taskName = unit.name();
        String unitId = unit.unitId();
        int taskIndex = taskContext.taskIndex();
        long submitTime = taskContext.submitTimeNanos();
        long startTime = taskContext.startTimeNanos();
        long endTime = taskContext.endTimeNanos();
        TaskCompletion<V> event = exception == null
                ? TaskCompletion.succeeded(taskName, unitId, taskIndex, submitTime, startTime, endTime, result)
                : TaskCompletion.failed(
                        taskName,
                        unitId,
                        taskIndex,
                        submitTime,
                        startTime,
                        endTime,
                        failureOutcome(unit.cancellationToken()),
                        exception);

        for (TaskListener listener : listeners) {
            try {
                listener.onTaskComplete(event);
            } catch (Throwable e) {
                logger.log(
                        Level.WARNING,
                        "TaskListener callback failed: " + listener.getClass().getName(),
                        e);
            }
        }
    }

    /**
     * Attributes a failed task from its token state at completion time. This is the direct
     * observation only: a task canceled under a {@code CANCELED} token reads {@code
     * MEMBER_CANCELED} here, while a group snapshot may later attribute the richer post-hoc cause
     * {@code GROUP_CANCELED} via {@link TokenOutcomes}.
     */
    private static TaskOutcome failureOutcome(CancellationToken token) {
        if (token.state() == CancellationToken.State.CANCELED) {
            return TaskOutcome.MEMBER_CANCELED;
        }
        return TokenOutcomes.forCanceled(token, TaskOutcome.USER_FAILURE);
    }

    @Override
    public String toString() {
        return "ScopedCallable{"
                + "taskName='"
                + taskContext.multiTaskContext().name()
                + '\''
                + ", delegate="
                + delegate
                + ", taskIndex="
                + taskContext.taskIndex()
                + ", submitTime="
                + taskContext.submitTimeNanos()
                + ", startTime="
                + taskContext.startTimeNanos()
                + ", endTime="
                + taskContext.endTimeNanos()
                + '}';
    }
}
