package io.github.monadrome.parallelinscope.scope;

import com.google.common.reflect.TypeToken;
import java.util.Objects;

/**
 * A typed key identifying one slot of a {@link TaskGroupDefinition}: a member or the terminal
 * combine.
 *
 * <p>A {@code TaskKey} captures the slot's result type at runtime through an anonymous subclass, in
 * the style of Guava's {@link TypeToken}:
 *
 * <pre>{@code
 * TaskKey<List<Order>> orders = new TaskKey<List<Order>>("orders") {};
 * }</pre>
 *
 * <p>The key is handed to {@link TaskGroupDefinition.Builder#task(TaskKey, ParName,
 * java.util.concurrent.Callable, MultiTaskOptions)} at configuration time. It carries no execution
 * state: the same key may be registered in several definitions and used against every submission of
 * those definitions, where it resolves a different {@link MultiTaskContext#unitId() unit} each time.
 * A key is configuration-time data, never an execution identity.
 *
 * <p>The name is the key's identity: keys are equal when they name the same slot, so a key whose
 * type parameter is a supertype of the registered result type is equal to the registered key and
 * resolves the same slot. The type parameter is a claim about the slot's result, validated when the
 * key is resolved by {@link TaskGroup#future(TaskKey)} — a claim that does not cover the registered
 * type fails there, not at construction.
 */
public abstract class TaskKey<T> {
    private final String name;
    private final TypeToken<T> resultType;

    protected TaskKey(String name) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty");
        }
        this.resultType = new TypeToken<T>(getClass()) {};
    }

    /** The slot name this key identifies; also the key's equality identity. */
    public final String name() {
        return name;
    }

    /** The result type captured from the anonymous subclass's type argument. */
    public final TypeToken<T> resultType() {
        return resultType;
    }

    /**
     * Keys are equal when they name the same slot. The captured result type is deliberately
     * excluded: {@link TaskGroup#future(TaskKey)} accepts a key claiming a supertype of the
     * registered type, so a key identifies the slot, not one particular type argument.
     */
    @Override
    public final boolean equals(Object other) {
        return this == other || (other instanceof TaskKey && name.equals(((TaskKey<?>) other).name));
    }

    @Override
    public final int hashCode() {
        return name.hashCode();
    }

    /** Diagnostics only; must not be persisted or used as a wire format. */
    @Override
    public String toString() {
        return name + " (" + resultType + ")";
    }
}
