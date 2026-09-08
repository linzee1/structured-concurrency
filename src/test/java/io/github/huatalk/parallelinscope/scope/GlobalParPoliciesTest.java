package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.spi.TaskListener;
import org.junit.jupiter.api.Test;

/** Builder validation matrix for {@link GlobalPar} policies and its task-listener overrides. */
class GlobalParPoliciesTest {

    @Test
    void parTaskListenerRejectsBlankNamesAndNullListenersAndAppendsPerPar() {
        GlobalPar.Builder builder = GlobalPar.builder();
        TaskListener first = event -> {};
        TaskListener second = event -> {};

        assertThat(builder.parTaskListener("orders", first)).isSameAs(builder);

        assertThatThrownBy(() -> builder.parTaskListener(null, first)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.parTaskListener("", first)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GlobalPar.builder().parTaskListener("fresh", null))
                .isInstanceOf(NullPointerException.class);

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("billing", executor)
                .taskListener(first)
                .parTaskListener("billing", first)
                .parTaskListener("billing", second)
                .build();
        try {
            assertThat(global.closed()).isFalse();
            assertThat(global.taskListeners()).containsExactly(first);
            assertThat(global.taskListenersFor("billing")).containsExactly(first, second);
            assertThatThrownBy(() -> global.taskListenersFor("billing").clear())
                    .isInstanceOf(UnsupportedOperationException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void taskListenerOverridesWithoutRegisteredNameFailBuildAndRegisterRejectsDuplicates() {
        assertThatThrownBy(() -> GlobalPar.builder()
                        .parTaskListener("ghost", event -> {})
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not registered");
        assertThatThrownBy(() -> GlobalPar.builder()
                        .register("same", java.util.concurrent.Executors.newSingleThreadExecutor())
                        .register("same", java.util.concurrent.Executors.newSingleThreadExecutor()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate Par name");
        assertThatThrownBy(() -> GlobalPar.builder().defaultPar("absent").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("default Par is not registered");
    }

    @Test
    void purgePolicyThresholdsAcceptBoundsOnly() {
        GlobalParPurgePolicy policy = GlobalParPurgePolicy.builder()
                .queuePressureThreshold(1.0)
                .canceledTaskRatioThreshold(1.0)
                .build();
        assertThat(policy).isNotNull();

        GlobalParPurgePolicy.Builder builder = GlobalParPurgePolicy.builder();
        assertThatThrownBy(() -> builder.queuePressureThreshold(1.0000001))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.queuePressureThreshold(0.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.queuePressureThreshold(-0.5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.queuePressureThreshold(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.canceledTaskRatioThreshold(1.5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.canceledTaskRatioThreshold(0d)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.canceledTaskRatioThreshold(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void purgePolicyGettersExposeConfiguredValuesExactly() {
        GlobalParPurgePolicy defaults = GlobalParPurgePolicy.builder().build();
        assertThat(defaults.enabled()).isFalse();
        assertThat(defaults.queuePressureThreshold()).isEqualTo(0.80d);
        assertThat(defaults.canceledTaskRatioThreshold()).isEqualTo(0.05d);

        GlobalParPurgePolicy custom = GlobalParPurgePolicy.builder()
                .enabled(true)
                .queuePressureThreshold(0.5d)
                .canceledTaskRatioThreshold(0.25d)
                .build();
        assertThat(custom.enabled()).isTrue();
        assertThat(custom.queuePressureThreshold()).isEqualTo(0.5d);
        assertThat(custom.canceledTaskRatioThreshold()).isEqualTo(0.25d);
    }

    @Test
    void deadlockPolicyAndTaskListenerBuildersExposeFluentSelfReturns() {
        GlobalParDeadlockPolicy.Builder deadlock = GlobalParDeadlockPolicy.builder();
        assertThat(deadlock.enabled(true)).isSameAs(deadlock);
        assertThat(deadlock.build().enabled()).isTrue();

        GlobalPar.Builder listeners = GlobalPar.builder();
        TaskListener listener = event -> {};
        assertThat(listeners.taskListener(listener)).isSameAs(listeners);
        GlobalPar global = listeners.build();
        try {
            assertThat(global.taskListeners()).containsExactly(listener);
        } finally {
            global.close();
        }
    }
}
