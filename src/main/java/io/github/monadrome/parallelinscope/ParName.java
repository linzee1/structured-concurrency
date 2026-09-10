package io.github.monadrome.parallelinscope;

import java.util.Objects;

/**
 * Logical name of one {@link Par} entry registered on a {@link GlobalPar}.
 *
 * <p>A {@code ParName} is a composition-root lookup key and diagnostic label, not a resource
 * identity. It names a logical execution entry; the physical pool is identified by {@link
 * ExecutorIdentity} through object reference. Within one {@code GlobalPar} a name resolves to exactly
 * one entry, and two differently named entries may intentionally share the same physical executor.
 *
 * <p>Instances are immutable value objects compared by {@link #value()}. Construction is the single
 * validation boundary: a name is never null and never blank, so {@link GlobalPar} and {@link
 * TaskGroupDefinition} no longer repeat that check. The value is used verbatim — no trimming,
 * lower-casing, or other normalization — so {@code of(" db ")} and {@code of("db")} are different
 * names and existing lookup semantics do not change.
 *
 * <p>A {@code ParName} proves only that the argument is a well-formed name; it cannot prove the name
 * is registered. {@code of("htpp")} is a valid value that fails at {@link GlobalPar.Builder#build()}
 * or {@link TaskGroup#submit(GlobalPar, TaskGroupDefinition)}. It must never participate in deadlock
 * or purge decisions, which are keyed on {@link ExecutorIdentity}.
 */
public final class ParName {
    private final String value;

    private ParName(String value) {
        this.value = value;
    }

    /**
     * Creates a name from a non-blank string.
     *
     * @param value the exact name to use as a lookup key
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code value} is blank
     */
    public static ParName of(String value) {
        Objects.requireNonNull(value, "value cannot be null");
        if (value.trim().isEmpty()) throw new IllegalArgumentException("Par name cannot be blank");
        return new ParName(value);
    }

    /** The exact name supplied at construction, without normalization. */
    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ParName && value.equals(((ParName) other).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /** Diagnostics only; must not be persisted or used as a wire format. */
    @Override
    public String toString() {
        return value;
    }
}
