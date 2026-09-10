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

    /** Creates a token whose deadline is {@code remaining} from now on the monotonic clock. */
    private static CancellationToken tokenExpiringIn(Duration remaining) {
        return new CancellationToken(null, System.nanoTime() + remaining.toNanos());
    }

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
    public void testBind_success_allFuturesComplete() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> f1 = SettableFuture.create();
        SettableFuture<String> f2 = SettableFuture.create();
        SettableFuture<String> f3 = SettableFuture.create();
        List<ListenableFuture<String>> futures = Arrays.asList(f1, f2, f3);

        token.bind(futures, Futures.immediateVoidFuture(), TIMER);

        f1.set("a");
        f2.set("b");
        f3.set("c");

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(token.state()).isEqualTo(CancellationToken.State.SUCCESS);
    }

    @Test
    public void testBind_timeout_stateTransitionsToTimeoutCanceled() throws Exception {
        SettableFuture<String> f1 = SettableFuture.create(); // never completed

        CancellationToken token = tokenExpiringIn(Duration.ofMillis(100));
        token.bind(ImmutableList.of(f1), Futures.immediateVoidFuture(), TIMER);

        // Wait for timeout to fire
        Thread.sleep(300);
        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT_CANCELED);
        assertThat(f1).isCancelled();
    }

    @Test
    public void testBind_deadlineAlreadyExpired_takesTimeoutPathSynchronously() throws Exception {
        SettableFuture<String> unfinished = SettableFuture.create();
        SettableFuture<String> finished = SettableFuture.create();
        finished.set("done");
        SettableFuture<Void> submitCanceller = SettableFuture.create();

        CancellationToken token = new CancellationToken(null, System.nanoTime() - 1L);
        token.bind(Arrays.asList(unfinished, finished), submitCanceller, TIMER);

        // The timeout path runs synchronously: unfinished work is cancelled, completed work keeps
        // its result (cancellation of a done future is a no-op), and the submitter is stopped.
        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT_CANCELED);
        assertThat(unfinished).isCancelled();
        assertThat(finished.get()).isEqualTo("done");
        assertThat(submitCanceller).isCancelled();
    }

    @Test
    public void testDeadlineInheritedFromParent_neverLaterThanParent() {
        long parentDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        CancellationToken parent = new CancellationToken(null, parentDeadline);

        // A child without its own deadline inherits the parent deadline.
        assertThat(new CancellationToken(parent).deadlineNanos()).isEqualTo(parentDeadline);
        // An explicit later child deadline is capped to the parent deadline.
        assertThat(new CancellationToken(parent, parentDeadline + 5L).deadlineNanos())
                .isEqualTo(parentDeadline);
        // An explicit earlier child deadline is honoured.
        assertThat(new CancellationToken(parent, parentDeadline - 5L).deadlineNanos())
                .isEqualTo(parentDeadline - 5L);
        // A root token has no deadline.
        assertThat(CancellationToken.create().deadlineNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testTimeoutCancel_recordsTimeoutBeforeCancellingBoundWork() {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();
        token.bind(ImmutableList.of(task), submitCanceller, TIMER);

        token.timeoutCancel();

        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT_CANCELED);
        assertThat(task).isCancelled();
        assertThat(submitCanceller).isCancelled();
    }

    @Test
    public void testStateListenerFiresSynchronouslyBeforeCancelAction() {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();
        StringBuilder order = new StringBuilder();
        token.addStateListener(state -> order.append("state=").append(state).append(';'));
        token.bind(ImmutableList.of(task), submitCanceller, TIMER);

        token.cancel(true);

        assertThat(order.toString()).startsWith("state=MUTUAL_CANCELED");
        // The cancel action already ran when the state listener fired; by the time cancel()
        // returns the future is cancelled.
        assertThat(task).isCancelled();
        assertThat(submitCanceller).isCancelled();
    }

    @Test
    public void testManualCancel_cancelsBoundFuturesAndSubmitCanceller() {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();
        token.bind(ImmutableList.of(task), submitCanceller, TIMER);

        token.cancel(true);

        assertThat(token.state()).isEqualTo(CancellationToken.State.MUTUAL_CANCELED);
        assertThat(task).isCancelled();
        assertThat(submitCanceller).isCancelled();
    }

    @Test
    public void testBind_failFast_oneFailsOthersCanceled() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> f1 = SettableFuture.create();
        SettableFuture<String> f2 = SettableFuture.create();
        List<ListenableFuture<String>> futures = Arrays.asList(f1, f2);

        // Priority 7: a failed future must transition the shared token into fail-fast cancellation.
        // This is the low-level state change that lets higher-level map calls stop sibling tasks.
        token.bind(futures, Futures.immediateVoidFuture(), TIMER);

        f1.setException(new RuntimeException("boom"));

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST_CANCELED);
    }

    @Test
    public void testBind_failFast_cancelsSiblingAndSubmitCanceller() {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> failed = SettableFuture.create();
        SettableFuture<String> sibling = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();

        token.bind(Arrays.asList(failed, sibling), submitCanceller, TIMER);

        failed.setException(new RuntimeException("boom"));

        await().untilAsserted(() -> {
            assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST_CANCELED);
            assertThat(sibling).isCancelled();
            assertThat(submitCanceller).isCancelled();
        });
    }

    @Test
    public void testBind_parentCanceled_childPropagates() throws Exception {
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        SettableFuture<String> f1 = SettableFuture.create();

        // Priority 9: nested scopes inherit cancellation from their parent.
        // Parent cancellation should mark the child as propagating cancellation even if its own
        // future has not completed yet.
        child.bind(ImmutableList.of(f1), Futures.immediateVoidFuture(), TIMER);

        parent.cancel(true);

        // Allow callback propagation
        Thread.sleep(50);
        assertThat(child.state()).isEqualTo(CancellationToken.State.PROPAGATING_CANCELED);
    }

    @Test
    public void testBind_parentAlreadyCanceled_childImmediatelyCanceled() {
        CancellationToken parent = CancellationToken.create();
        parent.cancel(true);
        assertThat(parent.state().shouldInterruptCurrentThread()).isTrue();

        CancellationToken child = new CancellationToken(parent);

        SettableFuture<String> f1 = SettableFuture.create();
        child.bind(ImmutableList.of(f1), Futures.immediateVoidFuture(), TIMER);

        // The future should be cancelled immediately because parent is already canceled
        assertThat(f1).isCancelled();
    }
}
