package io.github.huatalk.parallelinscope.scope;

import io.github.huatalk.parallelinscope.spi.TaskGroupListener;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Immutable option set for every multi-task entry point.
 *
 * <p>The same option set describes a {@code Par.map} batch, a {@code TaskGroupSpec} group level,
 * and a group member; each entry point reads the subset it owns:
 *
 * <ul>
 *   <li>A batch uses {@link #name()}, {@link #parallelism()}, {@link #timeout()}, {@link
 *       #taskType()}, and {@link #rejectEnqueue()}
 *   <li>A group level uses {@link #name()}, {@link #timeout()}, and {@link #listeners()};
 *       member-level execution strategy (parallelism, task type, enqueue policy) is per member
 *   <li>A group member reads the same fields as a batch except {@link #parallelism()}: a member is
 *       a single task, so its requested parallelism is resolved but never read. Nested work the
 *       member submits reads the parallelism of that nested submission's own options instead.
 * </ul>
 *
 * <p>The timeout is a forced explicit choice between two mutually exclusive builder declarations:
 * {@link Builder#timeout(Duration)} sets an explicit timeout, while {@link Builder#inheritTimeout()}
 * declares that the enclosing scope's deadline is inherited. {@link Builder#build()} rejects a
 * builder on which neither or both were called, so every call site must state its intent instead of
 * silently inheriting a global default.
 */
public final class MultiTaskOptions {
    private final String name;
    private final int parallelism;
    private final @Nullable Duration timeout;
    private final TaskType taskType;
    private final boolean rejectEnqueue;
    private final List<TaskGroupListener> listeners;

    private MultiTaskOptions(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name cannot be null");
        if (name.trim().isEmpty()) throw new IllegalArgumentException("name cannot be empty");
        if (!builder.timeoutDeclared && !builder.inheritDeclared) {
            throw new IllegalArgumentException("timeout is required; call timeout(Duration) or inheritTimeout()");
        }
        if (builder.timeoutDeclared && builder.inheritDeclared) {
            throw new IllegalArgumentException("timeout(Duration) and inheritTimeout() are mutually exclusive");
        }
        this.parallelism = builder.parallelism;
        this.timeout = builder.timeout;
        this.taskType = Objects.requireNonNull(builder.taskType, "taskType cannot be null");
        this.rejectEnqueue = builder.rejectEnqueue;
        this.listeners = Collections.unmodifiableList(new ArrayList<>(builder.listeners));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder of(String name) {
        return builder().name(name);
    }

    /** The batch, group, or member name. */
    public String name() {
        return name;
    }

    /**
     * Requested parallelism; non-positive means one worker per task. Ignored for task-group
     * members: a member is a single task, so nothing reads its resolved parallelism.
     */
    public int parallelism() {
        return parallelism;
    }

    /** The explicit execution timeout; empty means the enclosing scope's deadline is inherited. */
    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean rejectEnqueue() {
        return rejectEnqueue;
    }

    /** Group convergence listeners; only read by {@code TaskGroup}. */
    public List<TaskGroupListener> listeners() {
        return listeners;
    }

    /** Builder for immutable execution options. */
    public static final class Builder {
        private String name = "task";
        private int parallelism = -1;
        private @Nullable Duration timeout;
        private boolean timeoutDeclared;
        private boolean inheritDeclared;
        private TaskType taskType = TaskType.CPU_BOUND;
        private boolean rejectEnqueue = true;
        private final List<TaskGroupListener> listeners = new ArrayList<>();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /** Declares an explicit timeout; mutually exclusive with {@link #inheritTimeout()}. */
        public Builder timeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout cannot be null");
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("timeout must be positive when configured");
            }
            this.timeout = timeout;
            this.timeoutDeclared = true;
            return this;
        }

        /** Declares that the enclosing scope's deadline is inherited. */
        public Builder inheritTimeout() {
            this.inheritDeclared = true;
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

        public Builder listener(TaskGroupListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
            return this;
        }

        public MultiTaskOptions build() {
            return new MultiTaskOptions(this);
        }
    }
}
