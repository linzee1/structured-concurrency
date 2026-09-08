package io.github.huatalk.parallelinscope.context;

import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Thread-local batch state used only while submitting work to an executor.
 *
 * <p>A submission has a batch configuration but no current task. This scope lets a {@code
 * SmartBlockingQueue} apply the batch's enqueue policy before a worker begins executing a task.
 */
public final class SubmissionScope {
    private static final ThreadLocal<MultiTaskContext> CURRENT = new ThreadLocal<>();

    private SubmissionScope() {}

    /** Returns the batch currently being submitted, or null outside a submission. */
    public static @Nullable MultiTaskContext currentBatch() {
        return CURRENT.get();
    }

    /** Installs a batch for one submission and returns the batch it replaced. */
    public static @Nullable MultiTaskContext install(MultiTaskContext context) {
        MultiTaskContext previous = CURRENT.get();
        CURRENT.set(context);
        return previous;
    }

    /** Restores the batch returned from {@link #install(MultiTaskContext)}. */
    public static void restore(@Nullable MultiTaskContext context) {
        if (context == null) CURRENT.remove();
        else CURRENT.set(context);
    }
}
