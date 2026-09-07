package io.github.huatalk.parallelinscope.internal;

import io.github.huatalk.parallelinscope.cancel.CancellationToken;
import io.github.huatalk.parallelinscope.cancel.Checkpoints;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.TaskCompletion;
import io.github.huatalk.parallelinscope.scope.TaskOutcome;
import io.github.huatalk.parallelinscope.spi.TaskListener;
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
public class ScopedCallable<V> implements Callable<V> {

    private static final Logger logger = Logger.getLogger(ScopedCallable.class.getName());

    private final Callable<V> delegate;
    private final com.google.common.base.Ticker ticker;
    private final TaskExecutionContext taskContext;

    /** Creates a task wrapper from the batch context owned by one GlobalPar execution. */
    public ScopedCallable(TaskExecutionContext taskContext, Callable<V> delegate, List<TaskListener> taskListeners) {
        this.taskContext = Objects.requireNonNull(taskContext, "taskContext cannot be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
        this.ticker = com.google.common.base.Ticker.systemTicker();
        this.newTaskListeners = taskListeners == null ? java.util.Collections.emptyList() : taskListeners;
    }

    private List<TaskListener> newTaskListeners = java.util.Collections.emptyList();

    /**
     * Returns task execution time.
     *
     * @return the execution duration in nanoseconds
     */
    public long executionTime() {
        return taskContext.executionTimeNanos();
    }

    /**
     * Returns queue wait time.
     *
     * @return the queue wait duration in nanoseconds
     */
    public long waitTime() {
        return taskContext.waitTimeNanos();
    }

    /**
     * Returns total time from submission to completion.
     *
     * @return the total time in nanoseconds
     */
    public long totalTime() {
        return taskContext.totalTimeNanos();
    }

    /** Returns the per-task execution context owned by this wrapper. */
    public TaskExecutionContext taskExecutionContext() {
        return taskContext;
    }

    // ==================== Context Fields ====================

    /**
     * Returns this task's cancellation token.
     *
     * @return the task cancellation token
     */
    public CancellationToken cancellationToken() {
        return taskContext.multiTaskContext().cancellationToken();
    }

    /**
     * Returns the logical executor name.
     *
     * @return the executor name
     */
    public String executorName() {
        String executorLabel = taskContext.multiTaskContext().executorLabel();
        return executorLabel == null ? "NA" : executorLabel;
    }

    @Override
    public V call() throws Exception {
        // ==================== prepareContext ====================
        TaskExecutionContext previousTask = TaskExecutionContext.install(taskContext);

        MultiTaskContext unit = taskContext.multiTaskContext();
        String taskName = unit.name();
        // TaskGraphObservationScope is a TransmittableThreadLocal captured by the TtlCallable
        // wrapper created at the Par.map boundary.

        V result = null;
        Throwable taskException = null;
        try {
            // ==================== doCall ====================
            taskContext.markStarted(ticker.read());
            Checkpoints.checkpoint(taskName, true);
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
        List<TaskListener> listeners = newTaskListeners;
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
     * observation only; a group snapshot may later attribute a richer post-hoc cause.
     */
    private static TaskOutcome failureOutcome(CancellationToken token) {
        switch (token.state()) {
            case TIMEOUT:
                return TaskOutcome.TIMEOUT;
            case FAIL_FAST:
                return TaskOutcome.FAIL_FAST;
            case CANCELED:
                return TaskOutcome.MEMBER_CANCELED;
            case PROPAGATED_CANCELED:
                return token.originState() == CancellationToken.State.TIMEOUT
                        ? TaskOutcome.TIMEOUT
                        : TaskOutcome.GROUP_CANCELED;
            case SUCCESS:
            case RUNNING:
            default:
                return TaskOutcome.USER_FAILURE;
        }
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
