package io.github.huatalk.parallelinscope.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Boundary complements to {@link ActionGateTest}: exact open cadence across the invocation
 * counter and interval window boundaries.
 */
class ActionGateBoundaryTest {

    @Test
    void counterConsumesCallsBeforeTheIntervalWindowCanOpen() throws InterruptedException {
        ActionGate gate = ActionGate.whenBoth(3, Duration.ofMillis(60), () -> {});
        // First two calls only tick the counter down.
        assertThat(gate.due()).isFalse();
        assertThat(gate.due()).isFalse();
        // Counter exhausted, but the interval has not elapsed yet.
        assertThat(gate.due()).isFalse();

        Thread.sleep(70);
        assertThat(gate.due()).isTrue();

        // After opening, both boundaries reset: counter must refill and the clock restarts.
        assertThat(gate.due()).isFalse();
        Thread.sleep(70);
        assertThat(gate.due()).isFalse(); // One remaining call before the next open.
    }

    @Test
    void singleCallGateOpensOncePerIntervalWindow() throws InterruptedException {
        ActionGate gate = ActionGate.whenBoth(1, Duration.ofMillis(60), () -> {});
        // The window starts at construction; a call inside it stays closed.
        assertThat(gate.due()).isFalse();
        Thread.sleep(70);
        assertThat(gate.due()).isTrue(); // Window elapsed plus the one required call.
        assertThat(gate.due()).isFalse(); // Immediately closed again.
        Thread.sleep(70);
        assertThat(gate.due()).isTrue();
    }

    @Test
    void factoryValidatesArguments() {
        assertThatThrownBy(() -> ActionGate.whenBoth(0, Duration.ofMillis(1), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ActionGate.whenBoth(-1, Duration.ofMillis(1), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ActionGate.whenBoth(1, null, () -> {})).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ActionGate.whenBoth(1, Duration.ofMillis(1), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ActionGate.whenBoth(1, Duration.ZERO, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
