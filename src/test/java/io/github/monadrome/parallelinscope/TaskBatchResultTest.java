package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

/** Contract tests for {@link TaskBatchResult} reporting. */
public class TaskBatchResultTest {

    @Test
    public void report_classifiesMixedTerminalStatesAndSelectsFirstFailureByListOrder() {
        CancellationToken token = new CancellationToken();
        RuntimeException firstFailure = new RuntimeException("first failure");
        RuntimeException secondFailure = new RuntimeException("second failure");
        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(
                task(token, Futures.immediateCancelledFuture()),
                task(token, Futures.immediateFailedFuture(firstFailure)),
                task(token, Futures.immediateFuture("ok")),
                task(token, Futures.immediateFailedFuture(secondFailure))));

        TaskBatchResult.BatchReport report = batch.report();

        assertThat(report.stateCounts())
                .containsEntry(TaskOutcome.SUCCESS, 1)
                .containsEntry(TaskOutcome.USER_FAILURE, 2)
                .containsEntry(TaskOutcome.MEMBER_CANCELED, 1)
                .hasSize(3);
        assertThat(report.firstException()).isSameAs(firstFailure);
        assertThat(batch.reportString())
                .isEqualTo("SUCCESS:1,USER_FAILURE:2,MEMBER_CANCELED:1 | firstException=first failure");
    }

    @Test
    public void report_isSnapshotAndReflectsLaterCompletionOnlyInNewReport() {
        CancellationToken token = new CancellationToken();
        SettableFuture<String> pending = SettableFuture.create();
        TaskBatchResult<String> batch = TaskBatchResult.of(
                Arrays.asList(task(token, pending), task(token, Futures.immediateFuture("already done"))));

        TaskBatchResult.BatchReport beforeCompletion = batch.report();
        pending.set("now done");
        TaskBatchResult.BatchReport afterCompletion = batch.report();

        assertThat(beforeCompletion.stateCounts())
                .containsEntry(TaskOutcome.RUNNING, 1)
                .containsEntry(TaskOutcome.SUCCESS, 1);
        assertThat(afterCompletion.stateCounts())
                .containsOnlyKeys(TaskOutcome.SUCCESS)
                .containsEntry(TaskOutcome.SUCCESS, 2);
    }

    @Test
    public void report_emptyBatchHasNoStatesOrException() {
        TaskBatchResult<String> batch = TaskBatchResult.of(Collections.<TaskFuture<String>>emptyList());

        assertThat(batch.report().stateCounts()).isEmpty();
        assertThat(batch.report().firstException()).isNull();
        assertThat(batch.reportString()).isEmpty();
    }

    @Test
    public void report_deliversEveryElementAsATaskFuture() {
        CancellationToken token = new CancellationToken();
        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(
                task(token, Futures.immediateFuture("ok")), task(token, Futures.immediateCancelledFuture())));

        assertThat(batch.results()).allMatch(future -> future instanceof TaskFuture);
    }

    @Test
    public void results_areSnapshotIntoAnImmutableList() {
        CancellationToken token = new CancellationToken();
        java.util.List<TaskFuture<String>> mutable =
                new java.util.ArrayList<>(Collections.singletonList(task(token, Futures.immediateFuture("ok"))));
        TaskBatchResult<String> batch = TaskBatchResult.of(mutable);

        mutable.clear();

        assertThat(batch.results()).hasSize(1);
        assertThatThrownBy(() -> batch.results().add(task(token, Futures.immediateFuture("late"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void batchReport_defensivelyCopiesAndExposesUnmodifiableStateCounts() {
        Map<TaskOutcome, Integer> source = new java.util.EnumMap<>(TaskOutcome.class);
        source.put(TaskOutcome.SUCCESS, 1);
        TaskBatchResult.BatchReport report = new TaskBatchResult.BatchReport(source, null);

        source.put(TaskOutcome.USER_FAILURE, 1);

        assertThat(report.stateCounts()).containsOnlyKeys(TaskOutcome.SUCCESS).containsEntry(TaskOutcome.SUCCESS, 1);
        assertThatThrownBy(() -> report.stateCounts().put(TaskOutcome.MEMBER_CANCELED, 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void batchReport_acceptsNullStateCountsAndFormatsItsContents() {
        RuntimeException failure = new RuntimeException("failure");
        TaskBatchResult.BatchReport report = new TaskBatchResult.BatchReport(null, failure);

        assertThat(report.stateCounts()).isNull();
        assertThat(report.firstException()).isSameAs(failure);
        assertThat(report.toString()).contains("stateCounts=null", "firstException=");
    }

    // ==================== token-based cancellation attribution ====================

    private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void report_attributesCancelledElementsToBatchDeadlineTimeout() {
        CancellationToken token = new CancellationToken();
        token.timeoutCancel();
        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(
                task(token, Futures.<String>immediateCancelledFuture()),
                task(token, Futures.<String>immediateCancelledFuture())));

        assertThat(batch.report().stateCounts())
                .containsOnlyKeys(TaskOutcome.TIMEOUT)
                .containsEntry(TaskOutcome.TIMEOUT, 2);
    }

    @Test
    public void report_attributesCancelledSiblingsToFailFast() {
        CancellationToken token = new CancellationToken();
        Task<String> failed = task(token, Futures.immediateFailedFuture(new RuntimeException("boom")));
        Task<String> sibling = task(token, Futures.<String>immediateCancelledFuture());
        token.bind(Arrays.asList(failed, sibling), Futures.immediateVoidFuture(), TIMER);

        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(failed, sibling));

        assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST);
        assertThat(batch.report().stateCounts())
                .containsEntry(TaskOutcome.USER_FAILURE, 1)
                .containsEntry(TaskOutcome.FAIL_FAST, 1)
                .hasSize(2);
        assertThat(batch.report().firstException()).hasMessage("boom");
    }

    @Test
    public void report_attributesCancellationOfWholeBatchToGroupCanceled() {
        CancellationToken token = new CancellationToken();
        token.cancel();
        TaskBatchResult<String> batch =
                TaskBatchResult.of(Collections.singletonList(task(token, Futures.<String>immediateCancelledFuture())));

        assertThat(batch.report().stateCounts())
                .containsOnlyKeys(TaskOutcome.GROUP_CANCELED)
                .containsEntry(TaskOutcome.GROUP_CANCELED, 1);
    }

    @Test
    public void report_attributesPropagatedCancellationToItsOrigin() {
        CancellationToken timedOutParent = new CancellationToken();
        CancellationToken propagatedTimeout = new CancellationToken(timedOutParent);
        CancellationToken canceledParent = new CancellationToken();
        CancellationToken propagatedCancel = new CancellationToken(canceledParent);
        timedOutParent.timeoutCancel();
        canceledParent.cancel();

        assertThat(propagatedTimeout.state()).isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
        assertThat(propagatedCancel.state()).isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
        assertThat(TaskBatchResult.of(Collections.singletonList(
                                task(propagatedTimeout, Futures.<String>immediateCancelledFuture())))
                        .report()
                        .stateCounts())
                .containsOnlyKeys(TaskOutcome.TIMEOUT);
        assertThat(TaskBatchResult.of(Collections.singletonList(
                                task(propagatedCancel, Futures.<String>immediateCancelledFuture())))
                        .report()
                        .stateCounts())
                .containsOnlyKeys(TaskOutcome.GROUP_CANCELED);
    }

    @Test
    public void report_keepsDirectCancellationAsMemberCanceledWhenNoFrameworkPathCommitted() {
        CancellationToken token = new CancellationToken();
        TaskBatchResult<String> batch =
                TaskBatchResult.of(Collections.singletonList(task(token, Futures.<String>immediateCancelledFuture())));

        assertThat(token.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(batch.report().stateCounts())
                .containsOnlyKeys(TaskOutcome.MEMBER_CANCELED)
                .containsEntry(TaskOutcome.MEMBER_CANCELED, 1);
    }

    @Test
    public void report_attributesCancellationSignalFailuresThroughTheBatchToken() {
        CancellationToken token = new CancellationToken();
        token.timeoutCancel();
        // A checkpoint or interrupt can make the element future fail before the cascade cancel
        // lands on it; such failures are attributed through the token, not as user failures.
        Task<String> checkpointFailure =
                task(token, Futures.immediateFailedFuture(new LeanCancellationException("cancel during running")));
        Task<String> interrupted = task(token, Futures.immediateFailedFuture(new InterruptedException("interrupted")));
        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(checkpointFailure, interrupted));

        assertThat(batch.report().stateCounts())
                .containsOnlyKeys(TaskOutcome.TIMEOUT)
                .containsEntry(TaskOutcome.TIMEOUT, 2);
        assertThat(batch.report().firstException()).isNull();
    }

    @Test
    public void report_keepsSpontaneousCancellationSignalAsUserFailureWhenTokenUncommitted() {
        CancellationToken token = new CancellationToken();
        TaskBatchResult<String> batch = TaskBatchResult.of(Collections.singletonList(
                task(token, Futures.immediateFailedFuture(new LeanCancellationException("spontaneous")))));

        assertThat(token.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(batch.report().stateCounts())
                .containsOnlyKeys(TaskOutcome.USER_FAILURE)
                .containsEntry(TaskOutcome.USER_FAILURE, 1);
    }

    private static <T> Task<T> task(CancellationToken token, ListenableFuture<T> delegate) {
        return Task.of("batch", token, delegate);
    }
}
