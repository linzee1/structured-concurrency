package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.spi.TaskListener;
import org.junit.jupiter.api.Test;

/** Builder validation matrix for {@link GlobalPar} policies and its policy overrides. */
class GlobalParPoliciesTest {

    @Test
    void parPolicyOverrideRejectsBlankNamesDuplicatesAndNullPolicies() {
        GlobalPar.Builder builder = GlobalPar.builder();
        GlobalExecutionPolicy policy = GlobalExecutionPolicy.builder().build();

        assertThat(builder.parPolicyOverride("orders", policy)).isSameAs(builder);

        assertThatThrownBy(() -> builder.parPolicyOverride(null, policy)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.parPolicyOverride("", policy)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GlobalPar.builder().parPolicyOverride("fresh", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.parPolicyOverride("orders", policy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate");

        GlobalPar global = GlobalPar.builder()
                .register("billing", java.util.concurrent.Executors.newSingleThreadExecutor())
                .parPolicyOverride("billing", GlobalExecutionPolicy.builder().build())
                .build();
        try {
            assertThat(global.closed()).isFalse();
        } finally {
            global.close();
        }
    }

    @Test
    void policyOverridesWithoutRegisteredNameFailBuildAndRegisterRejectsDuplicates() {
        assertThatThrownBy(() -> GlobalPar.builder()
                        .parPolicyOverride(
                                "ghost", GlobalExecutionPolicy.builder().build())
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
    void deadlockPolicyAndExecutionPolicyBuildersExposeFluentSelfReturns() {
        GlobalParDeadlockPolicy.Builder deadlock = GlobalParDeadlockPolicy.builder();
        assertThat(deadlock.enabled(true)).isSameAs(deadlock);
        assertThat(deadlock.build().enabled()).isTrue();

        GlobalExecutionPolicy.Builder execution = GlobalExecutionPolicy.builder();
        TaskListener listener = event -> {};
        assertThat(execution.taskListener(listener)).isSameAs(execution);
        assertThat(execution.build().taskListeners()).containsExactly(listener);
    }
}
