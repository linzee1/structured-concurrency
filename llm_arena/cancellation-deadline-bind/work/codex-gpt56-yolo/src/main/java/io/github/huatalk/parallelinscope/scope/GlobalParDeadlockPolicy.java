package io.github.huatalk.parallelinscope.scope;

import io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;

/** Immutable deadlock policy owned by one GlobalPar. */
public final class GlobalParDeadlockPolicy {
    private final boolean enabled;
    private final List<DeadlockDetectionListener> listeners;

    private GlobalParDeadlockPolicy(Builder builder) {
        this.enabled = builder.enabled;
        IdentityHashMap<DeadlockDetectionListener, Boolean> seen = new IdentityHashMap<>();
        List<DeadlockDetectionListener> unique = new ArrayList<>();
        for (DeadlockDetectionListener listener : builder.listeners) {
            if (seen.put(listener, Boolean.TRUE) == null) unique.add(listener);
        }
        this.listeners = Collections.unmodifiableList(unique);
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean enabled() {
        return enabled;
    }

    public List<DeadlockDetectionListener> listeners() {
        return listeners;
    }

    public static final class Builder {
        private boolean enabled;
        private final List<DeadlockDetectionListener> listeners = new ArrayList<>();

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder listener(DeadlockDetectionListener listener) {
            listeners.add(java.util.Objects.requireNonNull(listener));
            return this;
        }

        public GlobalParDeadlockPolicy build() {
            return new GlobalParDeadlockPolicy(this);
        }
    }
}
