package io.github.huatalk.parallelinscope.scope;

/** Terminal reason for a parallel task group. */
public enum TaskGroupCompletionReason {
    SUCCESS,
    FAILED,
    TIMEOUT,
    CANCELED
}
