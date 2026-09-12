package io.github.monadrome.parallelinscope;

/**
 * Task type classification, determines scheduling behavior.
 *
 * <p>Used by {@link io.github.monadrome.parallelinscope.SmartBlockingQueue SmartBlockingQueue}
 * and the submission pipeline to control how tasks are queued and executed.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public enum TaskType {
    /** Network, RPC, database operations */
    IO_BOUND,
    /** Computation, validation, transformation, filtering */
    CPU_BOUND,
    /** Hybrid: e.g., cache check first, then IO on miss */
    MIXED
}
