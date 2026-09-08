package io.github.huatalk.parallelinscope.spi;

import io.github.huatalk.parallelinscope.scope.TaskCompletion;

/**
 * SPI: Task lifecycle listener for metrics collection and monitoring.
 *
 * <p>Implementations can record task execution times, queue wait times, etc. Register through
 * {@link
 * io.github.huatalk.parallelinscope.scope.GlobalPar.Builder#taskListener(TaskListener)}.
 *
 * <p>Each callback delivers a {@link TaskCompletion}: the same immutable record a task group
 * embeds in its result snapshot, here carrying the task result as well.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
@FunctionalInterface
public interface TaskListener {

    /**
     * Called when a task completes execution (both success and failure).
     *
     * @param event the completed task's identity, timing, outcome, and result
     */
    void onTaskComplete(TaskCompletion<?> event);
}
