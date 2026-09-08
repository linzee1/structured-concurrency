package io.github.monadrome.parallelinscope.scope;

import com.google.common.util.concurrent.Futures;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Read-only view of a group's successful member values, handed to the terminal {@link
 * CombineFunction}.
 *
 * <p>The view never blocks: the framework invokes the combine only after every member succeeded,
 * so each value is read from an already-completed member future. It exposes neither futures nor a
 * name-keyed map — values are looked up through the same typed {@link TaskRef} tokens used at
 * registration, keeping lookups type-safe and refactor-safe.
 *
 * <p>The view is valid only for the duration of the {@link CombineFunction#apply} call; the
 * framework may release result references after the callback returns.
 */
public final class CompletedTaskValues {

    private final Map<String, TaskGroup.MemberState> members;
    private final String combineName;

    CompletedTaskValues(Map<String, TaskGroup.MemberState> members, String combineName) {
        this.members = members;
        this.combineName = combineName;
    }

    /**
     * Returns the successful value of the given member without blocking; the value may be null.
     *
     * @throws IllegalArgumentException if the token names the combine itself or no member of this
     *     group, or if the token's raw result type is not assignable from the type the member was
     *     registered with (a token claiming a supertype of the registered type is accepted)
     */
    @SuppressWarnings("unchecked")
    public <T> T value(TaskRef<T> ref) {
        Objects.requireNonNull(ref, "ref cannot be null");
        if (ref.memberName().equals(combineName)) {
            throw new IllegalArgumentException("The combine cannot read its own ref '" + combineName + "'");
        }
        TaskGroup.MemberState member = members.get(ref.memberName());
        if (member == null) {
            throw new IllegalArgumentException("No member named '" + ref.memberName() + "'");
        }
        if (!ref.resultType().getRawType().isAssignableFrom(member.resultType.getRawType())) {
            throw new IllegalArgumentException("Member '" + ref.memberName() + "' was registered with result type "
                    + member.resultType + " but the ref claims " + ref.resultType());
        }
        try {
            return (T) Futures.getDone(member.future);
        } catch (ExecutionException | IllegalStateException failure) {
            // The combine runs only after every member succeeded, so anything but a plain value
            // signals a framework invariant violation, not user input. CancellationException is an
            // IllegalStateException and lands here too.
            throw new IllegalStateException(
                    "Member '" + ref.memberName() + "' has not completed successfully", failure);
        }
    }
}
