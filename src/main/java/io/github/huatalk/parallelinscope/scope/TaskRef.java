package io.github.huatalk.parallelinscope.scope;

import java.util.Objects;

/**
 * A typed token identifying one member of a {@link TaskGroupSpec}.
 *
 * <p>A {@code TaskRef} is returned by {@link TaskGroupSpec.Builder#task(String, String,
 * java.util.concurrent.Callable, MultiTaskOptions)} at configuration time. It carries no
 * execution state; after the spec is submitted, the same token resolves the member's future via
 * {@link TaskGroup#future(TaskRef)}.
 */
public final class TaskRef<T> {
    private final String memberName;

    TaskRef(String memberName) {
        this.memberName = Objects.requireNonNull(memberName, "memberName cannot be null");
    }

    /** The member name this token refers to. */
    public String memberName() {
        return memberName;
    }
}
