package io.github.monadrome.parallelinscope;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Immutable result wrapper for a batch of parallel tasks.
 *
 * <p>Bundles the list of individual {@link ListenableFuture} results together with a {@code
 * submitCanceller} future used to cancel ongoing submission.
 *
 * @param <T> the result type of individual tasks
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class TaskBatchResult<T> {

    private final @Nullable CancellationToken cancellationToken;
    private final ListenableFuture<?> submitCanceller;
    private final List<ListenableFuture<T>> results;

    private TaskBatchResult(
            @Nullable CancellationToken cancellationToken,
            @Nullable ListenableFuture<?> submitCanceller,
            List<ListenableFuture<T>> results) {
        this.cancellationToken = cancellationToken;
        this.submitCanceller = submitCanceller != null ? submitCanceller : Futures.immediateVoidFuture();
        this.results = results;
    }

    /**
     * Provides the future running the sliding-window submission loop. Cancelling it stops further
     * submissions and interrupts the submitter. Any unsubmitted placeholders then fail with the
     * interruption cause; canceling a placeholder directly completes it as {@code CANCELLED}.
     *
     * @return the submission-loop future
     */
    public ListenableFuture<?> submitCanceller() {
        return submitCanceller;
    }

    /**
     * Returns the futures for individual batch elements in input order.
     *
     * @return the individual result futures
     */
    public List<ListenableFuture<T>> results() {
        return results;
    }

    /**
     * Creates a result for a batch whose submissions may still be running.
     *
     * <p>Without the batch's cancellation token, cancellation attribution in {@link #report()}
     * stays coarse: every cancelled element reads {@link TaskOutcome#MEMBER_CANCELED}.
     *
     * @param <T> the element result type
     * @param submitCanceller the future running the remaining submissions
     * @param results the individual result futures
     * @return a new batch result
     */
    static <T> TaskBatchResult<T> of(ListenableFuture<?> submitCanceller, List<ListenableFuture<T>> results) {
        return new TaskBatchResult<>(null, submitCanceller, results);
    }

    /**
     * Creates a result for a fully submitted batch.
     *
     * <p>Without the batch's cancellation token, cancellation attribution in {@link #report()}
     * stays coarse: every cancelled element reads {@link TaskOutcome#MEMBER_CANCELED}.
     *
     * @param <T> the element result type
     * @param results the individual result futures
     * @return a new batch result
     */
    static <T> TaskBatchResult<T> of(List<ListenableFuture<T>> results) {
        return new TaskBatchResult<>(null, Futures.immediateVoidFuture(), results);
    }

    /**
     * Creates a result for a batch whose submissions may still be running, carrying the batch's
     * cancellation token so {@link #report()} attributes cancellations precisely.
     *
     * @param <T> the element result type
     * @param cancellationToken the token the batch's futures are bound to
     * @param submitCanceller the future running the remaining submissions
     * @param results the individual result futures
     * @return a new batch result
     */
    static <T> TaskBatchResult<T> of(
            CancellationToken cancellationToken,
            ListenableFuture<?> submitCanceller,
            List<ListenableFuture<T>> results) {
        return new TaskBatchResult<>(
                java.util.Objects.requireNonNull(cancellationToken, "cancellationToken cannot be null"),
                submitCanceller,
                results);
    }

    /**
     * Creates a result for a fully submitted batch, carrying the batch's cancellation token so
     * {@link #report()} attributes cancellations precisely.
     *
     * @param <T> the element result type
     * @param cancellationToken the token the batch's futures are bound to
     * @param results the individual result futures
     * @return a new batch result
     */
    static <T> TaskBatchResult<T> of(CancellationToken cancellationToken, List<ListenableFuture<T>> results) {
        return new TaskBatchResult<>(
                java.util.Objects.requireNonNull(cancellationToken, "cancellationToken cannot be null"),
                Futures.immediateVoidFuture(),
                results);
    }

    /**
     * Generates execution report: counts tasks by outcome and extracts first failure exception.
     *
     * <p>When this result carries the batch's cancellation token, cancelled elements are
     * attributed from the token's committed state (see {@link TokenOutcomes}): {@code TIMEOUT} for
     * deadline expiry, {@code FAIL_FAST} for the cascade after a sibling failure, {@code
     * GROUP_CANCELED} for batch-level or propagated cancellation, and {@code MEMBER_CANCELED} when
     * no framework path committed (a direct cancellation). A failure that merely signals observed
     * cancellation (a cooperative checkpoint or an interrupt racing the cascade cancel) is
     * attributed the same way instead of reading {@code USER_FAILURE}. The batch shares one token
     * across all elements, so an element whose direct cancellation <em>triggered</em> the fail-fast
     * cascade also reads {@code FAIL_FAST}; distinguishing the initiator per element requires a
     * {@code TaskGroup}.
     *
     * @return a BatchReport containing outcome counts and the first exception (if any)
     */
    public BatchReport report() {
        Map<TaskOutcome, Integer> outcomeMap = results.stream()
                .collect(Collectors.toMap(
                        this::outcomeOf, x -> 1, Integer::sum, () -> new EnumMap<>(TaskOutcome.class)));
        Throwable firstException = null;
        if (outcomeMap.containsKey(TaskOutcome.USER_FAILURE)
                || outcomeMap.containsKey(TaskOutcome.SUBMISSION_FAILURE)) {
            firstException = results.stream()
                    .filter(x -> {
                        TaskOutcome outcome = outcomeOf(x);
                        return outcome == TaskOutcome.USER_FAILURE || outcome == TaskOutcome.SUBMISSION_FAILURE;
                    })
                    .map(FutureInspector::exceptionNow)
                    .findFirst()
                    .orElse(null);
        }
        return new BatchReport(outcomeMap, firstException);
    }

    /**
     * Classifies one element's future, refining it through the batch token when one was supplied
     * at construction. Two future-layer results are refined: a cancelled future reads the token's
     * committed state, and so does a failure that merely signals observed cancellation (a
     * checkpoint or an interrupt can win the race against the cascade cancel on the element
     * future).
     */
    private TaskOutcome outcomeOf(ListenableFuture<T> future) {
        TaskOutcome outcome = FutureInspector.outcome(future);
        if (cancellationToken == null) {
            return outcome;
        }
        if (outcome == TaskOutcome.MEMBER_CANCELED) {
            return TokenOutcomes.forCanceled(cancellationToken, TaskOutcome.MEMBER_CANCELED);
        }
        if (outcome == TaskOutcome.USER_FAILURE
                && TokenOutcomes.causedByCancellation(FutureInspector.exceptionNow(future))) {
            return TokenOutcomes.forCanceled(cancellationToken, TaskOutcome.USER_FAILURE);
        }
        return outcome;
    }

    /**
     * Returns a human-readable summary string of the execution report.
     *
     * <p>Format: {@code STATE1:count,STATE2:count | firstException=message}
     *
     * @return formatted report string
     */
    public String reportString() {
        BatchReport r = report();
        Map<TaskOutcome, Integer> stateCounts = r.stateCounts();
        if (stateCounts == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<TaskOutcome, Integer> e : stateCounts.entrySet()) {
            if (!first) sb.append(',');
            sb.append(e.getKey()).append(':').append(e.getValue());
            first = false;
        }
        if (r.firstException() != null) {
            sb.append(" | firstException=").append(r.firstException().getMessage());
        }
        return sb.toString();
    }

    /** Immutable report of batch task execution state. */
    public static final class BatchReport {
        private final @Nullable Map<TaskOutcome, Integer> stateCounts;
        private final Throwable firstException;

        /**
         * Creates a batch report.
         *
         * @param stateCounts counts keyed by terminal or current future state
         * @param firstException the first observed failure, or {@code null}
         */
        public BatchReport(@Nullable Map<TaskOutcome, Integer> stateCounts, @Nullable Throwable firstException) {
            this.stateCounts = immutableStateCounts(stateCounts);
            this.firstException = firstException;
        }

        /**
         * Provides counts by future state, for example {@code SUCCESS=3, FAILED=1}.
         *
         * @return the immutable state count map, or {@code null} when unavailable
         */
        @Nullable
        public Map<TaskOutcome, Integer> stateCounts() {
            return stateCounts;
        }

        /**
         * Returns the first exception from failed tasks.
         *
         * @return the first failure, or {@code null} if no task failed
         */
        @Nullable
        public Throwable firstException() {
            return firstException;
        }

        @Override
        public String toString() {
            return "BatchReport{stateCounts=" + stateCounts + ", firstException=" + firstException + '}';
        }

        private static @Nullable Map<TaskOutcome, Integer> immutableStateCounts(
                @Nullable Map<TaskOutcome, Integer> stateCounts) {
            if (stateCounts == null) {
                return null;
            }
            EnumMap<TaskOutcome, Integer> copy = new EnumMap<>(TaskOutcome.class);
            copy.putAll(stateCounts);
            return Collections.unmodifiableMap(copy);
        }
    }
}
