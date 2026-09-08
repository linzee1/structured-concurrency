package io.github.huatalk.parallelinscope.spi;

import io.github.huatalk.parallelinscope.scope.TaskContext;
import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * SPI: Task lifecycle listener for metrics collection and monitoring.
 *
 * <p>Implementations can record task execution times, queue wait times, etc. Register through
 * {@link
 * io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy.Builder#taskListener(TaskListener)}.
 *
 * <p>Timing methods return {@link Duration}. Raw nanos timestamps are available via getters.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
@FunctionalInterface
public interface TaskListener {

    /**
     * Called when a task completes execution (both success and failure).
     *
     * @param event task execution event containing timing and metadata
     */
    void onTaskComplete(TaskEvent<?> event);

    /** Task context and outcome for one completed task. */
    final class TaskEvent<T> {
        private final TaskContext taskContext;
        private final T result;
        private final boolean successful;
        private final boolean enqueued;
        private final Throwable exception;

        private TaskEvent(
                TaskContext taskContext,
                @Nullable T result,
                boolean successful,
                boolean enqueued,
                @Nullable Throwable exception) {
            this.taskContext = Objects.requireNonNull(taskContext, "taskContext cannot be null");
            this.result = result;
            this.successful = successful;
            this.enqueued = enqueued;
            this.exception = exception;
        }

        /** Creates the completion event for a successful task, including a possibly-null result. */
        public static <T> TaskEvent<T> succeeded(TaskContext taskContext, @Nullable T result, boolean enqueued) {
            return new TaskEvent<>(taskContext, result, true, enqueued, null);
        }

        /** Creates the completion event for a failed task. */
        public static <T> TaskEvent<T> failed(TaskContext taskContext, Throwable exception, boolean enqueued) {
            return new TaskEvent<>(
                    taskContext, null, false, enqueued, Objects.requireNonNull(exception, "exception cannot be null"));
        }

        /** Returns the completed task's context. */
        public TaskContext taskContext() {
            return taskContext;
        }

        /**
         * Returns the logical task name.
         *
         * @return the logical task name
         */
        public String taskName() {
            return taskContext.batchContext().taskName();
        }

        /**
         * Gets the ticker reading at submission.
         *
         * @return the ticker reading in nanoseconds
         */
        public long submitTimeNanos() {
            return taskContext.submitTimeNanos();
        }

        /**
         * Gets the ticker reading at execution start.
         *
         * @return the ticker reading in nanoseconds
         */
        public long startTimeNanos() {
            return taskContext.startTimeNanos();
        }

        /**
         * Gets the ticker reading at completion.
         *
         * @return the ticker reading in nanoseconds
         */
        public long endTimeNanos() {
            return taskContext.endTimeNanos();
        }

        /** Returns whether the task completed successfully, including with a null result. */
        public boolean successful() {
            return successful;
        }

        /** Returns the task result, or null for a failed task or a successful null result. */
        public @Nullable T result() {
            return result;
        }

        /**
         * Checks whether the task was classified as queued.
         *
         * @return {@code true} if the measured queue wait exceeded the threshold
         */
        public boolean enqueued() {
            return enqueued;
        }

        /**
         * Gets the task failure.
         *
         * @return the failure, or {@code null} on success
         */
        @Nullable
        public Throwable exception() {
            return exception;
        }

        /**
         * Calculates the execution duration.
         *
         * @return the execution duration
         */
        public Duration executionTime() {
            return Duration.ofNanos(taskContext.executionTimeNanos());
        }

        /**
         * Calculates the queue wait duration.
         *
         * @return the queue wait duration
         */
        public Duration waitTime() {
            return Duration.ofNanos(taskContext.waitTimeNanos());
        }

        /**
         * Calculates the total duration.
         *
         * @return the duration from submission to completion
         */
        public Duration totalTime() {
            return Duration.ofNanos(taskContext.totalTimeNanos());
        }
    }
}
