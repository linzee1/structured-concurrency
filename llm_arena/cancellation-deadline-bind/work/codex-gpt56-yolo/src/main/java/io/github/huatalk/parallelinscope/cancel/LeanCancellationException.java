package io.github.huatalk.parallelinscope.cancel;

/**
 * Cancellation exception that omits its stack trace to reduce cancellation overhead.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class LeanCancellationException extends java.util.concurrent.CancellationException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a cancellation exception without retaining a stack trace.
     *
     * @param message the detail message
     */
    public LeanCancellationException(String message) {
        super(message);
        setStackTrace(new StackTraceElement[0]);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
