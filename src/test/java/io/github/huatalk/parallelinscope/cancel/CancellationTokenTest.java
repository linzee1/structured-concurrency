package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

/**
 * Tests for CancellationToken and cooperative cancellation.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class CancellationTokenTest {
    private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void testInitialState() {
        CancellationToken token = CancellationToken.create();
        assertThat(token.state()).isEqualTo(CancellationToken.State.RUNNING);
    }

    @Test
    public void testManualCancel() {
        CancellationToken token = CancellationToken.create();
        token.cancel(false);
        assertThat(token.state()).isEqualTo(CancellationToken.State.MUTUAL_CANCELED);
        assertThat(token.state().shouldInterruptCurrentThread()).isTrue();
    }

    @Test
    public void testParentChildChain() {
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        assertThat(parent.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(child.state()).isEqualTo(CancellationToken.State.RUNNING);

        parent.cancel(false);
        assertThat(parent.state()).isEqualTo(CancellationToken.State.MUTUAL_CANCELED);
    }

    @Test
    public void testCancellationTokenStateCodes() {
        assertThat(CancellationToken.State.RUNNING.code()).isZero();
        assertThat(CancellationToken.State.SUCCESS.code()).isEqualTo(1);
        assertThat(CancellationToken.State.RUNNING.shouldInterruptCurrentThread())
                .isFalse();
        assertThat(CancellationToken.State.SUCCESS.shouldInterruptCurrentThread())
                .isFalse();
        assertThat(CancellationToken.State.FAIL_FAST_CANCELED.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.TIMEOUT_CANCELED.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.MUTUAL_CANCELED.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.PROPAGATING_CANCELED.shouldInterruptCurrentThread())
                .isTrue();
    }

    // ==================== lateBind state transition tests ====================

    @Test
    public void testLateBind_success_allFuturesComplete() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> f1 = SettableFuture.create();
        SettableFuture<String> f2 = SettableFuture.create();
        SettableFuture<String> f3 = SettableFuture.create();
        List<ListenableFuture<String>> futures = Arrays.asList(f1, f2, f3);

        token.lateBind(futures, Duration.ofSeconds(5), Futures.immediateVoidFuture(), TIMER);

        f1.set("a");
        f2.set("b");
        f3.set("c");

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(token.state()).isEqualTo(CancellationToken.State.SUCCESS);
    }

    @Test
    public void testLateBind_timeout_stateTransitionsToTimeoutCanceled() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> f1 = SettableFuture.create(); // never completed

        token.lateBind(ImmutableList.of(f1), Duration.ofMillis(100), Futures.immediateVoidFuture(), TIMER);

        // Wait for timeout to fire
        Thread.sleep(300);
        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT_CANCELED);
    }

    @Test
    public void testLateBind_failFast_oneFailsOthersCanceled() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> f1 = SettableFuture.create();
        SettableFuture<String> f2 = SettableFuture.create();
        List<ListenableFuture<String>> futures = Arrays.asList(f1, f2);

        // Priority 7: a failed future must transition the shared token into fail-fast cancellation.
        // This is the low-level state change that lets higher-level map calls stop sibling tasks.
        token.lateBind(futures, Duration.ofSeconds(5), Futures.immediateVoidFuture(), TIMER);

        f1.setException(new RuntimeException("boom"));

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST_CANCELED);
    }

    @Test
    public void testLateBind_failFast_cancelsSiblingAndSubmitCanceller() {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> failed = SettableFuture.create();
        SettableFuture<String> sibling = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();

        token.lateBind(Arrays.asList(failed, sibling), Duration.ofSeconds(5), submitCanceller, TIMER);

        failed.setException(new RuntimeException("boom"));

        await().untilAsserted(() -> {
            assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST_CANCELED);
            assertThat(sibling).isCancelled();
            assertThat(submitCanceller).isCancelled();
        });
    }

    @Test
    public void testLateBind_parentCanceled_childPropagates() throws Exception {
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        SettableFuture<String> f1 = SettableFuture.create();

        // Priority 9: nested scopes inherit cancellation from their parent.
        // Parent cancellation should mark the child as propagating cancellation even if its own
        // future has not completed yet.
        child.lateBind(ImmutableList.of(f1), Duration.ofSeconds(5), Futures.immediateVoidFuture(), TIMER);

        parent.cancel(true);

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(child.state()).isEqualTo(CancellationToken.State.PROPAGATING_CANCELED);
    }

    @Test
    public void testLateBind_parentAlreadyCanceled_childImmediatelyCanceled() {
        CancellationToken parent = CancellationToken.create();
        parent.cancel(true);
        assertThat(parent.state().shouldInterruptCurrentThread()).isTrue();

        CancellationToken child = new CancellationToken(parent);

        SettableFuture<String> f1 = SettableFuture.create();
        child.lateBind(ImmutableList.of(f1), Duration.ofSeconds(5), Futures.immediateVoidFuture(), TIMER);

        // The future should be cancelled immediately because parent is already canceled
        assertThat(f1).isCancelled();
    }
}
