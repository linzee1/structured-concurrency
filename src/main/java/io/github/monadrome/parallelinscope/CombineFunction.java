package io.github.monadrome.parallelinscope;

/**
 * Terminal combine function of a task group: one business computation the framework runs after
 * every member has succeeded.
 *
 * <p>The function receives a {@link CompletedTaskValues} view exposing only successful member
 * values — never futures — so it cannot re-await, cancel, or orchestrate the underlying tasks. It
 * runs exactly once, on a worker thread of the {@code Par} named at registration, inside the same
 * scoped-task machinery as a member (execution context, TTL replay, deadline, cooperative
 * cancellation, listener events).
 *
 * <p>The function must be a pure function of member values and its configuration-time captures:
 * the framework schedules it the moment the last member succeeds, so there is no synchronization
 * edge between it and code the submitting thread runs after {@code TaskGroup.submit} returns. To
 * involve state created after submission, read the member futures and assemble outside the group
 * instead.
 *
 * <p>The {@code throws Exception} clause mirrors the member {@code Callable}: a checked failure is
 * recorded as {@link TaskOutcome#USER_FAILURE} with the original exception, exactly like a failed
 * member.
 *
 * @param <R> the assembled result type
 */
@FunctionalInterface
public interface CombineFunction<R> {

    /**
     * Computes the terminal value from the successful member values.
     *
     * @param values read-only view of the group's successful member values
     * @return the assembled terminal result, possibly null
     * @throws Exception any business failure, recorded as {@link TaskOutcome#USER_FAILURE}
     */
    R apply(CompletedTaskValues values) throws Exception;
}
