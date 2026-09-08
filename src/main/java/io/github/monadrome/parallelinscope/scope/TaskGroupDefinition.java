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
        private final TaskRef<T> ref;
        private final String executorName;
        private final Callable<T> callable;
        private final MultiTaskOptions options;

        private TaskDefinition(TaskRef<T> ref, String executorName, Callable<T> callable, MultiTaskOptions options) {
            this.ref = ref;
            this.executorName = executorName;
            this.callable = callable;
            this.options = options;
        }

        public String memberName() {
            return ref.memberName();
        }

        /** The token the member was registered with; carries the captured result type. */
        public TaskRef<T> ref() {
            return ref;
        }

        /** Name of the {@code Par} registered on the submitting {@code GlobalPar}. */
        public String executorName() {
            return executorName;
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
     * submitted only after all of them succeed. Its ref resolves the terminal future via {@link
     * TaskGroup#future(TaskRef)} like a member, but it is not part of {@link #tasks()}.
     */
    public static final class CombineDefinition<R> {
        private final TaskRef<R> ref;
        private final String executorName;
        private final CombineFunction<R> function;
        private final MultiTaskOptions options;

        private CombineDefinition(
                TaskRef<R> ref, String executorName, CombineFunction<R> function, MultiTaskOptions options) {
            this.ref = ref;
            this.executorName = executorName;
            this.function = function;
            this.options = options;
        }

        public String memberName() {
            return ref.memberName();
        }

        /** The token the combine was registered with; carries the captured result type. */
        public TaskRef<R> ref() {
            return ref;
        }

        /** Name of the {@code Par} registered on the submitting {@code GlobalPar}. */
        public String executorName() {
            return executorName;
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
         * Registers one member. The token carries the member name and captures its result type;
         * name nullness and blankness are rejected by the {@link TaskRef} constructor, while a
         * duplicate name is rejected here.
         */
        public <T> TaskRef<T> task(
                TaskRef<T> ref, String executorName, Callable<T> callable, MultiTaskOptions options) {
            Objects.requireNonNull(ref, "ref cannot be null");
            Objects.requireNonNull(executorName, "executorName cannot be null");
            Objects.requireNonNull(callable, "callable cannot be null");
            Objects.requireNonNull(options, "options cannot be null");
            if (tasks.containsKey(ref.memberName())) {
                throw new IllegalArgumentException("Duplicate memberName '" + ref.memberName() + "'");
            }
            tasks.put(ref.memberName(), new TaskDefinition<>(ref, executorName, callable, options));
            return ref;
        }

        /**
         * Registers the terminal combine. The combine is a real scoped task that depends on every
         * member: it is prepared at submit like a member but submitted to its executor only after
         * all members succeed, and the group completes only when its future is terminal. A group
         * accepts at most one combine; its name must not collide with any member name.
         */
        public <R> TaskRef<R> combine(
                TaskRef<R> ref, String executorName, CombineFunction<R> function, MultiTaskOptions options) {
            Objects.requireNonNull(ref, "ref cannot be null");
            Objects.requireNonNull(executorName, "executorName cannot be null");
            Objects.requireNonNull(function, "function cannot be null");
            Objects.requireNonNull(options, "options cannot be null");
            if (combine != null) {
                throw new IllegalArgumentException("A group accepts at most one combine");
            }
            if (tasks.containsKey(ref.memberName())) {
                throw new IllegalArgumentException("Duplicate memberName '" + ref.memberName() + "'");
            }
            combine = new CombineDefinition<>(ref, executorName, function, options);
            return ref;
        }

        public TaskGroupDefinition build() {
            return new TaskGroupDefinition(this);
        }
    }
}
