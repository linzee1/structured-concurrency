package io.github.huatalk.parallelinscope.scope;

import io.github.huatalk.parallelinscope.spi.TaskGroupListener;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/** Immutable options for one parallel task group. */
public final class TaskGroupOptions {
    private final String groupName;
    private final @Nullable Duration timeout;
    private final List<TaskGroupListener> listeners;

    private TaskGroupOptions(Builder builder) {
        this.groupName = Objects.requireNonNull(builder.groupName, "groupName cannot be null");
        if (groupName.trim().isEmpty()) throw new IllegalArgumentException("groupName cannot be empty");
        if (builder.timeout != null && (builder.timeout.isZero() || builder.timeout.isNegative())) {
            throw new IllegalArgumentException("timeout must be positive when configured");
        }
        this.timeout = builder.timeout;
        this.listeners = Collections.unmodifiableList(new ArrayList<>(builder.listeners));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder of(String groupName) {
        return builder().groupName(groupName);
    }

    public String groupName() {
        return groupName;
    }

    public @Nullable Duration timeout() {
        return timeout;
    }

    public List<TaskGroupListener> listeners() {
        return listeners;
    }

    /** Builder for immutable group options. */
    public static final class Builder {
        private String groupName = "task-group";
        private @Nullable Duration timeout;
        private final List<TaskGroupListener> listeners = new ArrayList<>();

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder timeout(@Nullable Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder listener(TaskGroupListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
            return this;
        }

        public TaskGroupOptions build() {
            return new TaskGroupOptions(this);
        }
    }
}
