package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Contract pins for {@link BatchOptions}: the batch identity, the requested parallelism, the
 * per-batch execution policy, and how those resolve into a {@link MultiTaskContext}.
 */
class BatchOptionsTest {

    @Test
    void defaultsAreOneWorkerPerTaskCpuBoundAndRejecting() {
        BatchOptions options = BatchOptions.timeout("load", Duration.ofSeconds(30));

        assertThat(options.name()).isEqualTo("load");
        assertThat(options.parallelism()).isEqualTo(-1);
        assertThat(options.taskType()).isEqualTo(TaskType.CPU_BOUND);
        assertThat(options.rejectEnqueue()).isTrue();
    }

    @Test
    void withersRoundTripEveryFieldWithoutMutatingTheOriginal() {
        BatchOptions base = BatchOptions.inheritTimeout("load");

        BatchOptions derived = base.parallelism(3).taskType(TaskType.IO_BOUND).rejectEnqueue(false);

        assertThat(base.parallelism()).isEqualTo(-1);
        assertThat(base.taskType()).isEqualTo(TaskType.CPU_BOUND);
        assertThat(base.rejectEnqueue()).isTrue();
        assertThat(derived.parallelism()).isEqualTo(3);
        assertThat(derived.taskType()).isEqualTo(TaskType.IO_BOUND);
        assertThat(derived.rejectEnqueue()).isFalse();
        assertThat(derived.timeout()).isEmpty();
    }

    @Test
    void nameIsValidatedOnceAndPreservedVerbatim() {
        String longName = "order-pipeline-stage-7";

        assertThat(BatchOptions.timeout(longName, Duration.ofSeconds(30)).name())
                .isEqualTo(longName);
        assertThatThrownBy(() -> BatchOptions.inheritTimeout(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BatchOptions.inheritTimeout("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveExplicitTimeouts() {
        assertThatThrownBy(() -> BatchOptions.timeout("load", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchOptions.timeout("load", Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchOptions.timeout("load", null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void explicitTimeoutAndParallelismResolveIntoTheBatchContext() {
        MultiTaskContext context = MultiTaskContext.resolve(
                BatchOptions.timeout("write", Duration.ofSeconds(3))
                        .parallelism(8)
                        .taskType(TaskType.IO_BOUND)
                        .rejectEnqueue(false)
                        .spec(),
                2,
                null);

        assertThat(context.name()).isEqualTo("write");
        assertThat(context.taskCount()).isEqualTo(2);
        assertThat(context.effectiveParallelism()).isEqualTo(2);
        assertThat(context.taskType()).isEqualTo(TaskType.IO_BOUND);
        assertThat(context.rejectEnqueue()).isFalse();
        assertThat(context.remaining().toMillis()).isLessThanOrEqualTo(3_000);
    }

    @Test
    void nonPositiveParallelismMeansOneWorkerPerTask() {
        MultiTaskContext context = MultiTaskContext.resolve(
                BatchOptions.timeout("read", Duration.ofSeconds(3)).spec(), 4, null);

        assertThat(context.effectiveParallelism()).isEqualTo(4);
    }

    @Test
    void inheritedTimeoutWithoutParentIsRejectedAtResolution() {
        assertThatThrownBy(() -> MultiTaskContext.resolve(
                        BatchOptions.inheritTimeout("read").spec(), 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no enclosing deadline to inherit");
    }

    @Test
    void inheritedTimeoutResolvesToTheParentDeadline() {
        MultiTaskContext parent = MultiTaskContext.resolve(
                BatchOptions.timeout("outer", Duration.ofMillis(100)).spec(), 1, null);
        MultiTaskContext child =
                MultiTaskContext.resolve(BatchOptions.inheritTimeout("inner").spec(), 1, parent);

        assertThat(child.deadlineNanos()).isEqualTo(parent.deadlineNanos());
    }
}
