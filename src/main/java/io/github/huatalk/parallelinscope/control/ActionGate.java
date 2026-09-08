package io.github.huatalk.parallelinscope.control;

import java.time.Duration;
import java.util.Objects;

/**
 * Controls when an action is due based on invocation count, elapsed time, or both.
 *
 * <p>The gate can either bind an action at construction or report when an action is due. It does
 * not block, reject, delay, or schedule invocations. Gate state is safe for concurrent callers, but
 * actions run outside the state lock and may overlap when a later boundary opens before an earlier
 * action completes.
 *
 * <pre>{@code
 * ActionGate purgeGate = ActionGate.whenBoth(100, Duration.ofSeconds(1));
 * purgeGate.runIfDue(executor::purge);
 * }</pre>
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class ActionGate {

    private final int minInvocations;
    private final long minIntervalNanos;
    private final Runnable action;
    private int remainingInvocations;
    private long lastOpenTimeNanos;

    private ActionGate(int minInvocations, Duration minInterval, Runnable action) {
        if (minInterval != null && (minInterval.isZero() || minInterval.isNegative())) {
            throw new IllegalArgumentException("minInterval must be positive");
        }
        this.minInvocations = minInvocations;
        this.remainingInvocations = minInvocations;
        this.minIntervalNanos = minInterval == null ? 0 : minInterval.toNanos();
        this.lastOpenTimeNanos = minInterval == null ? 0 : System.nanoTime();
        this.action = action;
    }

    /**
     * Creates an unbound gate that opens every {@code invocations} invocations.
     *
     * @param invocations number of invocations between openings
     * @return a count-based gate
     */
    public static ActionGate every(int invocations) {
        requirePositive(invocations, "invocations");
        return new ActionGate(invocations, null, null);
    }

    /**
     * Creates a gate that runs the supplied action every {@code invocations} invocations.
     *
     * @param invocations number of invocations between action runs
     * @param action action to run
     * @return a count-based gate
     */
    public static ActionGate every(int invocations, Runnable action) {
        requirePositive(invocations, "invocations");
        return new ActionGate(invocations, null, Objects.requireNonNull(action, "action"));
    }

    /**
     * Creates an unbound gate that opens after each interval.
     *
     * @param interval minimum duration between openings
     * @return a time-based gate
     */
    public static ActionGate every(Duration interval) {
        return new ActionGate(0, Objects.requireNonNull(interval, "interval"), null);
    }

    /**
     * Creates a gate that runs the supplied action after each interval.
     *
     * @param interval minimum duration between action runs
     * @param action action to run
     * @return a time-based gate
     */
    public static ActionGate every(Duration interval, Runnable action) {
        return new ActionGate(
                0, Objects.requireNonNull(interval, "interval"), Objects.requireNonNull(action, "action"));
    }

    /**
     * Creates an unbound gate that requires both invocation and time boundaries.
     *
     * @param minInvocations minimum number of invocations between openings
     * @param minInterval minimum duration between openings
     * @return a gate using both boundaries
     */
    public static ActionGate whenBoth(int minInvocations, Duration minInterval) {
        requirePositive(minInvocations, "minInvocations");
        return new ActionGate(minInvocations, Objects.requireNonNull(minInterval, "minInterval"), null);
    }

    /**
     * Creates a gate that runs the supplied action when both boundaries are reached.
     *
     * @param minInvocations minimum number of invocations between action runs
     * @param minInterval minimum duration between action runs
     * @param action action to run
     * @return a gate using both boundaries
     */
    public static ActionGate whenBoth(int minInvocations, Duration minInterval, Runnable action) {
        requirePositive(minInvocations, "minInvocations");
        return new ActionGate(
                minInvocations,
                Objects.requireNonNull(minInterval, "minInterval"),
                Objects.requireNonNull(action, "action"));
    }

    /**
     * Returns whether the configured invocation and time boundaries have been reached.
     *
     * <p>An open result consumes the current boundary and starts the next interval.
     *
     * @return {@code true} when an action is due
     */
    public synchronized boolean due() {
        if (minInvocations > 0) {
            if (remainingInvocations > 0 && --remainingInvocations > 0) {
                return false;
            }
            if (minIntervalNanos == 0) {
                remainingInvocations = minInvocations;
                return true;
            }
        }

        long now = System.nanoTime();
        if (now - lastOpenTimeNanos < minIntervalNanos) {
            return false;
        }
        remainingInvocations = minInvocations;
        lastOpenTimeNanos = now;
        return true;
    }

    /**
     * Runs the action bound at construction when the configured boundaries are reached. The boundary
     * is consumed before the action runs and is not restored when the action fails.
     *
     * @return {@code true} when the bound action ran
     * @throws IllegalStateException if no action was bound at construction
     * @throws RuntimeException if the bound action fails
     */
    public boolean runIfDue() {
        if (action == null) {
            throw new IllegalStateException("no action is bound");
        }
        return runIfDue(action);
    }

    /**
     * Runs the supplied action when the configured boundaries are reached. The boundary is consumed
     * before the action runs and is not restored when the action fails.
     *
     * @param candidate action to run
     * @return {@code true} when the supplied action ran
     * @throws RuntimeException if the supplied action fails
     */
    public boolean runIfDue(Runnable candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (!due()) {
            return false;
        }
        candidate.run();
        return true;
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
