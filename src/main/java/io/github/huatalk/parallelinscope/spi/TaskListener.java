package io.github.huatalk.parallelinscope.spi;

import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * SPI: Task lifecycle listener for metrics collection and monitoring.
 *
 * <p>Implementations can record task execution times, queue wait times, etc. Register through
 * {@link
 * io.github.huatalk.parallelinscope.scope.GlobalPar.Builder#taskListener(TaskListener)}.
 *
 * <p>Timing methods return {@link Duration}. Raw nanos timestamps are available via accessors.
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

    /** Identity, timing, and outcome for one completed task. */
    final class TaskEvent<T> {
        private final String taskName;
        private final String unitId;
        private final int taskIndex;
        private final long submitTimeNanos;
        private final long startTimeNanos;
        private final long endTimeNanos;
        private final T result;
        private final boolean successful;
        private final boolean enqueued;
        private final Throwable exception;

        private TaskEvent(
                String taskName,
                String unitId,
                int taskIndex,
                long submitTimeNanos,
                long startTimeNanos,
                long endTimeNanos,
                @Nullable T result,
                boolean successful,
                boolean enqueued,
                @Nullable Throwable exception) {
            this.taskName = Objects.requireNonNull(taskName, "taskName cannot be null");
            this.unitId = Objects.requireNonNull(unitId, "unitId cannot be null");
            if (taskIndex < 0) throw new IllegalArgumentException("taskIndex must not be negative");
            this.taskIndex = taskIndex;
            this.submitTimeNanos = submitTimeNanos;
            this.startTimeNanos = startTimeNanos;
            this.endTimeNanos = endTimeNanos;
            this.result = result;
            this.successful = successful;
            this.enqueued = enqueued;
            this.exception = exception;
        }

        /** Creates the completion event for a successful task, including a possibly-null result. */
        public static <T> TaskEvent<T> succeeded(
                String taskName,
                String unitId,
                int taskIndex,
                long submitTimeNanos,
                long startTimeNanos,
                long endTimeNanos,
                @Nullable T result,
                boolean enqueued) {
            return new TaskEvent<>(
                    taskName,
                    unitId,
                    taskIndex,
                    submitTimeNanos,
                    startTimeNanos,
                    endTimeNanos,
                    result,
                    true,
                    enqueued,
                    null);
        }

        /** Creates the completion event for a failed task. */
        public static <T> TaskEvent<T> failed(
                String taskName,
                String unitId,
                int taskIndex,
                long submitTimeNanos,
                long startTimeNanos,
                long endTimeNanos,
                Throwable exception,
                boolean enqueued) {
            return new TaskEvent<>(
                    taskName,
                    unitId,
                    taskIndex,
                    submitTimeNanos,
                    startTimeNanos,
                    endTimeNanos,
                    null,
                    false,
                    enqueued,
                    Objects.requireNonNull(exception, "exception cannot be null"));
        }

        /**
         * Returns the logical task name.
         *
         * @return the logical task name
         */
        public String taskName() {
            return taskName;
        }

        /** Returns the stable identity of the multi-task unit invocation that owns this task. */
        public String unitId() {
            return unitId;
        }

        /** Returns the stable index of this task's input element within its unit. */
        public int taskIndex() {
            return taskIndex;
        }

        /**
         * Gets the ticker reading at submission.
         *
         * @return the ticker reading in nanoseconds
         */
        public long submitTimeNanos() {
            return submitTimeNanos;
        }

        /**
         * Gets the ticker reading at execution start.
         *
         * @return the ticker reading in nanoseconds
         */
        public long startTimeNanos() {
            return startTimeNanos;
        }

        /**
         * Gets the ticker reading at completion.
         *
         * @return the ticker reading in nanoseconds
         */
        public long endTimeNanos() {
            return endTimeNanos;
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
            return Duration.ofNanos(endTimeNanos - startTimeNanos);
        }

        /**
         * Calculates the queue wait duration.
         *
         * @return the queue wait duration
         */
        public Duration waitTime() {
            return Duration.ofNanos(startTimeNanos - submitTimeNanos);
        }

        /**
         * Calculates the total duration.
         *
         * @return the duration from submission to completion
         */
        public Duration totalTime() {
            return Duration.ofNanos(endTimeNanos - submitTimeNanos);
        }
    }
}
