package io.github.monadrome.parallelinscope.scope;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Identity key for one exact supplied executor object.
 *
 * <p>Equality deliberately uses reference equality, not {@link Object#equals(Object)}.
 * This prevents independent wrappers or value-like executors from being merged as one resource. The
 * textual form is diagnostics only and must not be persisted or used as a graph key.
 *
 * <p>Identity is always keyed on the supplied executor, never on a derived object. When the supplied
 * executor is not already a Guava {@code ListeningExecutorService}, the framework submits through a
 * {@code listeningDecorator} adapter that exists only to obtain {@code ListenableFuture}s. That
 * adapter is a per-registration view of the same resource, not a pool of its own, so keying on it
 * would split one physical executor into several identities and corrupt identity-based runtime
 * merging, executor-graph edges, and deadlock detection.
 */
public final class ExecutorIdentity {
    private final ExecutorService supplied;
    private final int hash;

    public ExecutorIdentity(ExecutorService supplied) {
        this.supplied = Objects.requireNonNull(supplied, "supplied executor cannot be null");
        this.hash = System.identityHashCode(supplied);
    }

    public ExecutorService suppliedExecutor() {
        return supplied;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ExecutorIdentity && ((ExecutorIdentity) other).supplied == supplied;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return supplied.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(supplied));
    }
}
