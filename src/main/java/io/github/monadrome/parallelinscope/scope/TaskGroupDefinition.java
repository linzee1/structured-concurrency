package io.github.monadrome.parallelinscope.scope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Immutable, reusable, pure-data description of one heterogeneous task group.
 *
 * <p>A definition captures only configuration: the group-level {@link MultiTaskOptions} (of which the
 * group reads name, timeout, and listeners), the ordered member definitions, and an optional terminal
 * combine. It binds no thread context, executor, or deadline; those are resolved from the submitting
 * environment at each {@link TaskGroup#submit(GlobalPar, TaskGroupDefinition)} call, so one definition
 * can be submitted repeatedly.
 */
public final class TaskGroupDefinition {
    private final MultiTaskOptions groupOptions;
    private final List<TaskDefinition<?>> tasks;
    private final @Nullable CombineDefinition<?> combine;

    private TaskGroupDefinition(Builder builder) {
        this.groupOptions = builder.groupOptions;
        this.tasks = Collections.unmodifiableList(new ArrayList<>(builder.tasks.values()));
        this.combine = builder.combine;
    }

    public static Builder builder(MultiTaskOptions groupOptions) {
        return new Builder(groupOptions);
    }

    /** Group-level options; the group reads name, timeout, and listeners. */
    public MultiTaskOptions groupOptions() {
        return groupOptions;
    }

    /** Ordered task definitions; an empty list describes an immediately successful group. */
    public List<TaskDefinition<?>> tasks() {
        return tasks;
    }

    /** The terminal combine definition, or null when the group declares none. */
    public @Nullable CombineDefinition<?> combine() {
        return combine;
    }

    /** Immutable description of one group member. */
    public static final class TaskDefinition<T> {
        private final TaskKey<T> key;
        private final ParName parName;
        private final Callable<T> callable;
        private final MultiTaskOptions options;

        private TaskDefinition(TaskKey<T> key, ParName parName, Callable<T> callable, MultiTaskOptions options) {
            this.key = key;
            this.parName = parName;
            this.callable = callable;
            this.options = options;
        }

        public String memberName() {
            return key.memberName();
        }

        /** The key the member was registered with; carries the captured result type. */
        public TaskKey<T> key() {
            return key;
        }

        /**
         * Logical {@code Par} name resolved against the submitting {@code GlobalPar} at submit time.
         *
         * <p>The name is intentionally unresolved here: a definition is pure data that binds no
         * executor and can be submitted to any topology.
         */
        public ParName parName() {
            return parName;
        }

        public Callable<T> callable() {
            return callable;
        }

        public MultiTaskOptions options() {
            return options;
        }
    }

    /**
     * Immutable description of the terminal combine: one task that depends on every member and is
     * submitted only after all of them succeed. Its key resolves the terminal future via {@link
     * TaskGroup#future(TaskKey)} like a member, but it is not part of {@link #tasks()}.
     */
    public static final class CombineDefinition<R> {
        private final TaskKey<R> key;
        private final ParName parName;
        private final CombineFunction<R> function;
        private final MultiTaskOptions options;

        private CombineDefinition(
                TaskKey<R> key, ParName parName, CombineFunction<R> function, MultiTaskOptions options) {
            this.key = key;
            this.parName = parName;
            this.function = function;
            this.options = options;
        }

        public String memberName() {
            return key.memberName();
        }

        /** The key the combine was registered with; carries the captured result type. */
        public TaskKey<R> key() {
            return key;
        }

        /** Logical {@code Par} name resolved against the submitting {@code GlobalPar} at submit time. */
        public ParName parName() {
            return parName;
        }

        public CombineFunction<R> function() {
            return function;
        }

        public MultiTaskOptions options() {
            return options;
        }
    }

    /** Configuration builder; each {@link #task} call is validated immediately. */
    public static final class Builder {
        private final MultiTaskOptions groupOptions;
        private final LinkedHashMap<String, TaskDefinition<?>> tasks = new LinkedHashMap<>();
        private @Nullable CombineDefinition<?> combine;

        private Builder(MultiTaskOptions groupOptions) {
            this.groupOptions = Objects.requireNonNull(groupOptions, "groupOptions cannot be null");
        }

        /**
         * Registers one member. The key carries the member name and captures its result type;
         * name nullness and blankness are rejected by the {@link TaskKey} constructor, while a
         * duplicate name is rejected here. The {@code parName} is validated by {@link
         * ParName#of(String)} and resolved against the submitting {@link GlobalPar} at submit time.
         */
        public <T> TaskKey<T> task(TaskKey<T> key, ParName parName, Callable<T> callable, MultiTaskOptions options) {
            Objects.requireNonNull(key, "key cannot be null");
            Objects.requireNonNull(parName, "parName cannot be null");
            Objects.requireNonNull(callable, "callable cannot be null");
            Objects.requireNonNull(options, "options cannot be null");
            if (tasks.containsKey(key.memberName())) {
                throw new IllegalArgumentException("Duplicate memberName '" + key.memberName() + "'");
            }
            tasks.put(key.memberName(), new TaskDefinition<>(key, parName, callable, options));
            return key;
        }

        /**
         * Registers the terminal combine. The combine is a real scoped task that depends on every
         * member: it is prepared at submit like a member but submitted to its executor only after
         * all members succeed, and the group completes only when its future is terminal. A group
         * accepts at most one combine; its name must not collide with any member name.
         */
        public <R> TaskKey<R> combine(
                TaskKey<R> key, ParName parName, CombineFunction<R> function, MultiTaskOptions options) {
            Objects.requireNonNull(key, "key cannot be null");
            Objects.requireNonNull(parName, "parName cannot be null");
            Objects.requireNonNull(function, "function cannot be null");
            Objects.requireNonNull(options, "options cannot be null");
            if (combine != null) {
                throw new IllegalArgumentException("A group accepts at most one combine");
            }
            if (tasks.containsKey(key.memberName())) {
                throw new IllegalArgumentException("Duplicate memberName '" + key.memberName() + "'");
            }
            combine = new CombineDefinition<>(key, parName, function, options);
            return key;
        }

        public TaskGroupDefinition build() {
            return new TaskGroupDefinition(this);
        }
    }
}
