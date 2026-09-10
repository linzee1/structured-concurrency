package io.github.huatalk.parallelinscope.scope;

import io.github.huatalk.parallelinscope.spi.TaskListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable application-level defaults shared by all {@link Par} entries in one {@link GlobalPar}.
 *
 * <p>This is declarative configuration only: it does not register executors, own schedulers, or
 * retain batch state. A per-Par override replaces this policy for that entry during batch
 * resolution; it does not create another executor or resource-ownership scope.
 */
public final class GlobalExecutionPolicy {
    private final long defaultTimeoutMillis;
    private final List<TaskListener> taskListeners;

    private GlobalExecutionPolicy(Builder builder) {
        this.defaultTimeoutMillis = builder.defaultTimeoutMillis;
        this.taskListeners = Collections.unmodifiableList(new ArrayList<>(builder.taskListeners));
    }

    public static Builder builder() {
        return new Builder();
    }

    public long defaultTimeoutMillis() {
        return defaultTimeoutMillis;
    }

    /**
     * Returns an immutable listener snapshot. Listener callbacks run on task execution paths and must
     * therefore be non-blocking and tolerate concurrent invocation.
     */
    public List<TaskListener> taskListeners() {
        return taskListeners;
    }

    public static final class Builder {
        private long defaultTimeoutMillis = 60_000L;
        private final List<TaskListener> taskListeners = new ArrayList<>();

        public Builder defaultTimeoutMillis(long timeoutMillis) {
            if (timeoutMillis <= 0) throw new IllegalArgumentException("timeout must be positive");
            this.defaultTimeoutMillis = timeoutMillis;
            return this;
        }

        public Builder taskListener(TaskListener listener) {
            taskListeners.add(java.util.Objects.requireNonNull(listener));
            return this;
        }

        public GlobalExecutionPolicy build() {
            return new GlobalExecutionPolicy(this);
        }
    }
}
