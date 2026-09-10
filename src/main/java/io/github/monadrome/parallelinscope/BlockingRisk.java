package io.github.monadrome.parallelinscope;

/** Conservative blocking capability of a supplied executor. */
enum BlockingRisk {
    UNKNOWN,
    BOUNDED_PLATFORM_POOL,
    VIRTUAL_THREAD_PER_TASK,
    UNBOUNDED
}
