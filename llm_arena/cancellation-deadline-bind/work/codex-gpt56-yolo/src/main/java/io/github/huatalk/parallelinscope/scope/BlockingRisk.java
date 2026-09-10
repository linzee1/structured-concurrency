package io.github.huatalk.parallelinscope.scope;

/** Conservative blocking capability of a supplied executor. */
public enum BlockingRisk {
    UNKNOWN,
    BOUNDED_PLATFORM_POOL,
    VIRTUAL_THREAD_PER_TASK,
    UNBOUNDED
}
