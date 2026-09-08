package io.github.huatalk.parallelinscope.scope;

import com.google.common.reflect.TypeToken;
import java.util.Objects;

/**
 * A typed token identifying one member of a {@link TaskGroupSpec}.
 *
 * <p>A {@code TaskRef} captures the member's result type at runtime through an anonymous subclass,
 * in the style of Guava's {@link TypeToken}:
 *
 * <pre>{@code
 * TaskRef<List<Order>> orders = new TaskRef<List<Order>>("orders") {};
 * }</pre>
 *
 * <p>The token is handed to {@link TaskGroupSpec.Builder#task(TaskRef, String,
 * java.util.concurrent.Callable, MultiTaskOptions)} at configuration time. It carries no execution
 * state; after the spec is submitted, the same token resolves the member's future via {@link
 * TaskGroup#future(TaskRef)}, which rejects a token whose raw result type does not cover the type
 * the member was registered with.
 */
public abstract class TaskRef<T> {
    private final String memberName;
    private final TypeToken<T> resultType;

    protected TaskRef(String memberName) {
        this.memberName = Objects.requireNonNull(memberName, "memberName cannot be null");
        if (memberName.trim().isEmpty()) {
            throw new IllegalArgumentException("memberName cannot be empty");
        }
        this.resultType = new TypeToken<T>(getClass()) {};
    }

    /** The member name this token refers to. */
    public final String memberName() {
        return memberName;
    }

    /** The result type captured from the anonymous subclass's type argument. */
    public final TypeToken<T> resultType() {
        return resultType;
    }
}
