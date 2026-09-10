package io.github.huatalk.parallelinscope.scope;

/**
 * Immutable thread-pool-level purge policy owned by one {@link GlobalPar}.
 *
 * <p>The thresholds are advisory trigger conditions, not queue-accounting guarantees. They apply
 * once per physical supplied executor even when several named {@code Par} entries share it.
 */
public final class GlobalParPurgePolicy {
    private final boolean enabled;
    private final double queuePressureThreshold;
    private final double canceledTaskRatioThreshold;

    private GlobalParPurgePolicy(Builder builder) {
        this.enabled = builder.enabled;
        this.queuePressureThreshold = builder.queuePressureThreshold;
        this.canceledTaskRatioThreshold = builder.canceledTaskRatioThreshold;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean enabled() {
        return enabled;
    }

    public double queuePressureThreshold() {
        return queuePressureThreshold;
    }

    public double canceledTaskRatioThreshold() {
        return canceledTaskRatioThreshold;
    }

    public static final class Builder {
        private boolean enabled;
        private double queuePressureThreshold = 0.80;
        private double canceledTaskRatioThreshold = 0.05;

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder queuePressureThreshold(double threshold) {
            validate(threshold, "queuePressureThreshold");
            this.queuePressureThreshold = threshold;
            return this;
        }

        public Builder canceledTaskRatioThreshold(double threshold) {
            validate(threshold, "canceledTaskRatioThreshold");
            this.canceledTaskRatioThreshold = threshold;
            return this;
        }

        public GlobalParPurgePolicy build() {
            return new GlobalParPurgePolicy(this);
        }

        private static void validate(double value, String name) {
            if (Double.isNaN(value) || value <= 0.0 || value > 1.0) {
                throw new IllegalArgumentException(name + " must be in (0, 1]");
            }
        }
    }
}
