package io.github.huatalk.parallelinscope.spi;

/**
 * A best-effort execution phase reported by a submitted future.
 *
 * <p>These values describe which lifecycle transition won inside the future. They do not prove
 * executor queue membership or that user code has started running.
 */
public enum ExecutionPhase {
    /** No call to {@code run()} or {@code cancel()} has claimed the future yet. */
    SUBMITTED,
    /** The future's {@code run()} method claimed execution. */
    RUNNING,
    /** Cancellation won before {@code run()} claimed execution. */
    CANCELLED_BEFORE_RUN,
    /** Cancellation succeeded after {@code run()} claimed execution. */
    CANCEL_REQUESTED_RUNNING,
    /** The future's {@code run()} method returned. */
    TERMINAL
}
