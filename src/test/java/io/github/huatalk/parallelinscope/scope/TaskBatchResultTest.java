package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Contract tests for {@link TaskBatchResult} reporting. */
public class TaskBatchResultTest {

    @Test
    public void report_classifiesMixedTerminalStatesAndSelectsFirstFailureByListOrder() {
        RuntimeException firstFailure = new RuntimeException("first failure");
        RuntimeException secondFailure = new RuntimeException("second failure");
        TaskBatchResult<String> batch = TaskBatchResult.of(Arrays.asList(
                Futures.immediateCancelledFuture(),
                Futures.immediateFailedFuture(firstFailure),
                Futures.immediateFuture("ok"),
                Futures.immediateFailedFuture(secondFailure)));

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
        SettableFuture<String> pending = SettableFuture.create();
        TaskBatchResult<String> batch =
                TaskBatchResult.of(Arrays.asList(pending, Futures.immediateFuture("already done")));

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
        TaskBatchResult<String> batch = TaskBatchResult.of(Collections.<ListenableFuture<String>>emptyList());

        assertThat(batch.report().stateCounts()).isEmpty();
        assertThat(batch.report().firstException()).isNull();
        assertThat(batch.reportString()).isEmpty();
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
}
