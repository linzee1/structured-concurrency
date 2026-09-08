package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class MultiTaskOptionsTest {
    @Test
    void remainsAnImmutablePerCallInput() {
        MultiTaskOptions options = MultiTaskOptions.of("load")
                .parallelism(3)
                .timeout(Duration.ofSeconds(2))
                .taskType(TaskType.IO_BOUND)
                .build();

        assertThat(options.name()).isEqualTo("load");
        assertThat(options.parallelism()).isEqualTo(3);
        assertThat(options.timeout()).contains(Duration.ofSeconds(2));
        assertThat(options.taskType()).isEqualTo(TaskType.IO_BOUND);
    }

    @Test
    void timeoutRequiresExactlyOneDeclaration() {
        assertThatThrownBy(() -> MultiTaskOptions.of("read").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout(Duration) or inheritTimeout()");
        assertThatThrownBy(() -> MultiTaskOptions.of("read")
                        .timeout(Duration.ofSeconds(1))
                        .inheritTimeout()
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
        assertThatThrownBy(() -> MultiTaskOptions.of("read")
                        .inheritTimeout()
                        .timeout(Duration.ofSeconds(1))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void inheritTimeoutYieldsAnEmptyTimeoutAccessor() {
        assertThat(MultiTaskOptions.of("read").inheritTimeout().build().timeout())
                .isEmpty();
    }

    @Test
    void rejectsNonPositiveExplicitTimeouts() {
        assertThatThrownBy(() -> MultiTaskOptions.of("load").timeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MultiTaskOptions.of("load").timeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MultiTaskOptions.of("load").timeout(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void repeatedTimeoutDeclarationsKeepTheLastValue() {
        MultiTaskOptions options = MultiTaskOptions.of("load")
                .timeout(Duration.ofSeconds(1))
                .timeout(Duration.ofSeconds(5))
                .build();

        assertThat(options.timeout()).contains(Duration.ofSeconds(5));
    }

    @Test
    void retainsAllExecutionHints() {
        MultiTaskOptions options = MultiTaskOptions.of("write")
                .parallelism(7)
                .taskType(TaskType.IO_BOUND)
                .rejectEnqueue(false)
                .timeout(Duration.ofSeconds(30))
                .build();

        assertThat(options.parallelism()).isEqualTo(7);
        assertThat(options.rejectEnqueue()).isFalse();
        assertThat(options.timeout()).contains(Duration.ofSeconds(30));
    }

    @Test
    void resolvesExplicitTimeoutAndParallelismIntoBatchContext() {
        MultiTaskContext context = MultiTaskContext.resolve(
                MultiTaskOptions.of("write")
                        .parallelism(8)
                        .timeout(Duration.ofSeconds(3))
                        .taskType(TaskType.IO_BOUND)
                        .rejectEnqueue(false)
                        .build(),
                2,
                null);

        assertThat(context.taskName()).isEqualTo("write");
        assertThat(context.effectiveParallelism()).isEqualTo(2);
        assertThat(context.taskType()).isEqualTo(TaskType.IO_BOUND);
        assertThat(context.rejectEnqueue()).isFalse();
    }

    @Test
    void resolvesExplicitTimeoutIntoBatchContext() {
        MultiTaskContext context = MultiTaskContext.resolve(
                MultiTaskOptions.of("read")
                        .parallelism(-1)
                        .timeout(Duration.ofSeconds(3))
                        .build(),
                4,
                null);

        assertThat(context.effectiveParallelism()).isEqualTo(4);
        assertThat(context.remaining().toMillis()).isLessThanOrEqualTo(3_000);
    }

    @Test
    void inheritedTimeoutWithoutParentIsRejectedAtResolution() {
        assertThatThrownBy(() -> MultiTaskContext.resolve(
                        MultiTaskOptions.of("read").inheritTimeout().build(), 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no enclosing deadline to inherit");
    }

    @Test
    void inheritedTimeoutResolvesToTheParentDeadline() {
        MultiTaskContext parent = MultiTaskContext.resolve(
                MultiTaskOptions.of("outer").timeout(Duration.ofMillis(100)).build(), 1, null);
        MultiTaskContext child = MultiTaskContext.resolve(
                MultiTaskOptions.of("inner").inheritTimeout().build(), 1, parent);

        assertThat(child.deadlineNanos()).isEqualTo(parent.deadlineNanos());
    }
}
