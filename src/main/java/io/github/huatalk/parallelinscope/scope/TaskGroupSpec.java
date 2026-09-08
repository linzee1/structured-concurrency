package io.github.huatalk.parallelinscope.scope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Immutable, reusable, pure-data description of one heterogeneous task group.
 *
 * <p>A spec captures only configuration: the group-level {@link MultiTaskOptions} (of which the
 * group reads name, timeout, and listeners) and the ordered member definitions. It binds no thread
 * context, executor, or deadline; those are resolved from the submitting environment at each
 * {@link TaskGroup#submit(GlobalPar, TaskGroupSpec)} call, so one spec can be submitted
 * repeatedly.
 */
public final class TaskGroupSpec {
    private final MultiTaskOptions groupOptions;
    private final List<MemberSpec<?>> members;

    private TaskGroupSpec(Builder builder) {
        this.groupOptions = builder.groupOptions;
        this.members = Collections.unmodifiableList(new ArrayList<>(builder.members.values()));
    }

    public static Builder builder(MultiTaskOptions groupOptions) {
        return new Builder(groupOptions);
    }

    /** Group-level options; the group reads name, timeout, and listeners. */
    public MultiTaskOptions groupOptions() {
        return groupOptions;
    }

    /** Ordered member definitions; an empty list describes an immediately successful group. */
    public List<MemberSpec<?>> members() {
        return members;
    }

    /** Immutable description of one group member. */
    public static final class MemberSpec<T> {
        private final String memberName;
        private final String executorName;
        private final Callable<T> callable;
        private final MultiTaskOptions options;

        private MemberSpec(String memberName, String executorName, Callable<T> callable, MultiTaskOptions options) {
            this.memberName = memberName;
            this.executorName = executorName;
            this.callable = callable;
            this.options = options;
        }

        public String memberName() {
            return memberName;
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

    /** Configuration builder; each {@link #task} call is validated immediately. */
    public static final class Builder {
        private final MultiTaskOptions groupOptions;
        private final LinkedHashMap<String, MemberSpec<?>> members = new LinkedHashMap<>();

        private Builder(MultiTaskOptions groupOptions) {
            this.groupOptions = Objects.requireNonNull(groupOptions, "groupOptions cannot be null");
        }

        public <T> TaskRef<T> task(
                String memberName, String executorName, Callable<T> callable, MultiTaskOptions options) {
            Objects.requireNonNull(memberName, "memberName cannot be null");
            if (memberName.trim().isEmpty()) throw new IllegalArgumentException("memberName cannot be empty");
            Objects.requireNonNull(executorName, "executorName cannot be null");
            Objects.requireNonNull(callable, "callable cannot be null");
            Objects.requireNonNull(options, "options cannot be null");
            if (members.containsKey(memberName)) {
                throw new IllegalArgumentException("Duplicate memberName '" + memberName + "'");
            }
            members.put(memberName, new MemberSpec<>(memberName, executorName, callable, options));
            return new TaskRef<>(memberName);
        }

        public TaskGroupSpec build() {
            return new TaskGroupSpec(this);
        }
    }
}
