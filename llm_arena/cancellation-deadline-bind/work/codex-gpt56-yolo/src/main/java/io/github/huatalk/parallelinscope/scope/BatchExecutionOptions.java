package io.github.huatalk.parallelinscope.scope;

import java.time.Duration;
import java.util.Objects;

/** Immutable per-batch execution request. */
public final class BatchExecutionOptions {
    private final String taskName;
    private final int parallelism;
    private final Duration timeout;
    private final TaskType taskType;
    private final boolean rejectEnqueue;

    private BatchExecutionOptions(Builder builder) {
        this.taskName = Objects.requireNonNull(builder.taskName, "taskName cannot be null");
        this.parallelism = builder.parallelism;
        if (builder.timeout != null && (builder.timeout.isNegative() || builder.timeout.isZero())) {
            throw new IllegalArgumentException("timeout must be positive when configured");
        }
        this.timeout = builder.timeout;
        this.taskType = Objects.requireNonNull(builder.taskType, "taskType cannot be null");
        this.rejectEnqueue = builder.rejectEnqueue;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder of(String taskName) {
        return builder().taskName(taskName);
    }

    public String taskName() {
        return taskName;
    }

    public int parallelism() {
        return parallelism;
    }

    public Duration timeout() {
        return timeout;
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean rejectEnqueue() {
        return rejectEnqueue;
    }

    public static final class Builder {
        private String taskName = "task";
        private int parallelism = -1;
        private Duration timeout;
        private TaskType taskType = TaskType.CPU_BOUND;
        private boolean rejectEnqueue = true;

        public Builder taskName(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder timeout(Duration timeout) {
            if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
                throw new IllegalArgumentException("timeout must be positive when configured");
            }
            this.timeout = timeout;
            return this;
        }

        public Builder taskType(TaskType taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder rejectEnqueue(boolean rejectEnqueue) {
            this.rejectEnqueue = rejectEnqueue;
            return this;
        }

        public BatchExecutionOptions build() {
            return new BatchExecutionOptions(this);
        }
    }
}
