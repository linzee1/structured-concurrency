package io.github.huatalk.parallelinscope.scope;

/** Read-only identity and timing context for one task in a batch or one task-group member. */
public interface TaskContext {

    /** Returns the batch that owns this task. */
    MultiTaskContext batchContext();

    /** Returns the stable index of this task's input element within its batch. */
    int taskIndex();

    long submitTimeNanos();

    long startTimeNanos();

    long endTimeNanos();

    long executionTimeNanos();

    long waitTimeNanos();

    long totalTimeNanos();
}
