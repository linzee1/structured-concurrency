package io.github.huatalk.parallelinscope.internal;

/**
 * Identifies a failure that occurred while handing a prepared task to its executor, before user
 * code started. {@code TaskGroup} classifies member failures by this type, so it must stay
 * a distinct shared type rather than being wrapped anonymously.
 */
public final class SubmissionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SubmissionException(Throwable cause) {
        super("Task submission failed", cause);
    }
}
