package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for CancellationToken and cooperative cancellation.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class CancellationTokenTest {
    private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor();

    private static CancellationToken withDeadlineAfter(long millis) {
        return new CancellationToken(null, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis));
    }

    @Test
    public void testInitialState() {
        CancellationToken token = CancellationToken.create();
        assertThat(token.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(token.deadlineNanos()).isEqualTo(Long.MAX_VALUE);
        assertThat(token.remaining().toNanos()).isGreaterThan(TimeUnit.HOURS.toNanos(1));
    }

    @Test
    public void testManualCancel() {
        CancellationToken token = CancellationToken.create();
        token.cancel(false);
        assertThat(token.state()).isEqualTo(CancellationToken.State.CANCELED);
        assertThat(token.state().shouldInterruptCurrentThread()).isTrue();
    }

    @Test
    public void testParentChildChain() {
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        assertThat(parent.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(child.state()).isEqualTo(CancellationToken.State.RUNNING);

        parent.cancel(false);
        assertThat(parent.state()).isEqualTo(CancellationToken.State.CANCELED);
    }

    @Test
    public void testCancellationTokenStateCodes() {
        assertThat(CancellationToken.State.RUNNING.code()).isZero();
        assertThat(CancellationToken.State.SUCCESS.code()).isEqualTo(1);
        assertThat(CancellationToken.State.RUNNING.shouldInterruptCurrentThread())
                .isFalse();
        assertThat(CancellationToken.State.SUCCESS.shouldInterruptCurrentThread())
                .isFalse();
        assertThat(CancellationToken.State.FAIL_FAST.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.TIMEOUT.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.CANCELED.shouldInterruptCurrentThread())
                .isTrue();
        assertThat(CancellationToken.State.PROPAGATED_CANCELED.shouldInterruptCurrentThread())
                .isTrue();
    }

    // ==================== deadline ====================

    @Test
    public void deadlineIsCappedByParentDeadline() {
        long later = System.nanoTime() + TimeUnit.HOURS.toNanos(1);
        long earlier = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);

        CancellationToken parent = new CancellationToken(null, earlier);
        CancellationToken childRequestingLater = new CancellationToken(parent, later);
        CancellationToken childRequestingEarlier = new CancellationToken(parent, earlier - 1);

        assertThat(childRequestingLater.deadlineNanos()).isEqualTo(parent.deadlineNanos());
        assertThat(childRequestingEarlier.deadlineNanos()).isEqualTo(earlier - 1);
    }

    @Test
    public void expiredDeadlineCancelsBoundWorkThroughTheTimeoutChain() throws Exception {
        CancellationToken token = withDeadlineAfter(-1000);

        SettableFuture<String> pending = SettableFuture.create();
        SettableFuture<String> alreadySucceeded = SettableFuture.create();
        alreadySucceeded.set("kept");
        SettableFuture<Void> submitCanceller = SettableFuture.create();

        token.bind(Arrays.asList(pending, alreadySucceeded), submitCanceller, TIMER);

        await().untilAsserted(() -> {
            assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT);
            assertThat(pending).isCancelled();
            assertThat(submitCanceller).isCancelled();
        });
        assertThat(alreadySucceeded).isNotCancelled();
        assertThat(alreadySucceeded.get()).isEqualTo("kept");
    }

    // ==================== bind state transition tests ====================

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
        CancellationToken token = withDeadlineAfter(100);

        SettableFuture<String> f1 = SettableFuture.create(); // never completed

        token.bind(ImmutableList.of(f1), Futures.immediateVoidFuture(), TIMER);

        // Wait for timeout to fire
        Thread.sleep(300);
        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT);
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
        assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST);
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
            assertThat(token.state()).isEqualTo(CancellationToken.State.FAIL_FAST);
            assertThat(sibling).isCancelled();
            assertThat(submitCanceller).isCancelled();
        });
    }

    @Test
    public void testBind_manualCancel_cancelsBoundWorkAndSubmitCanceller() {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> task = SettableFuture.create();
        SettableFuture<Void> submitCanceller = SettableFuture.create();

        token.bind(ImmutableList.of(task), submitCanceller, TIMER);

        token.cancel(true);

        assertThat(token.state()).isEqualTo(CancellationToken.State.CANCELED);
        assertThat(task).isCancelled();
        assertThat(submitCanceller).isCancelled();
    }

    @Test
    public void testBind_cancelAfterSuccess_keepsRecordedResult() throws Exception {
        CancellationToken token = CancellationToken.create();

        SettableFuture<String> task = SettableFuture.create();
        token.bind(ImmutableList.of(task), Futures.immediateVoidFuture(), TIMER);
        task.set("done");

        await().until(() -> token.state() == CancellationToken.State.SUCCESS);

        token.cancel(true);

        assertThat(task.isDone()).isTrue();
        assertThat(task.get()).isEqualTo("done");
        assertThat(token.state()).isEqualTo(CancellationToken.State.SUCCESS);
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
        assertThat(child.state()).isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
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

    @Test
    public void timeoutCancelTransitionsAndCancelsBoundWork() {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        token.bind(ImmutableList.of(task), Futures.immediateVoidFuture(), TIMER);

        token.timeoutCancel();

        assertThat(token.state()).isEqualTo(CancellationToken.State.TIMEOUT);
        await().until(task::isCancelled);
    }

    @Test
    public void losingStateTransitionDoesNotNotifyListeners() {
        CancellationToken token = CancellationToken.create();
        java.util.concurrent.atomic.AtomicInteger notifications = new java.util.concurrent.atomic.AtomicInteger();
        token.addStateListener(state -> notifications.incrementAndGet());

        token.cancel(true);
        token.timeoutCancel(); // loses the CAS: already CANCELED
        token.cancel(true); // loses again

        assertThat(token.state()).isEqualTo(CancellationToken.State.CANCELED);
        assertThat(notifications).hasValue(1);
    }

    @Test
    public void stateListenerRunsAfterTransitionBeforeCancellation() {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        token.bind(ImmutableList.of(task), Futures.immediateVoidFuture(), TIMER);

        java.util.concurrent.atomic.AtomicReference<CancellationToken.State> observed =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean taskStillPending = new java.util.concurrent.atomic.AtomicBoolean();
        token.addStateListener(state -> {
            observed.set(state);
            taskStillPending.set(!task.isDone());
        });

        token.cancel(true);

        assertThat(observed.get()).isEqualTo(CancellationToken.State.CANCELED);
        assertThat(taskStillPending).isTrue();
        assertThat(task).isCancelled();
    }

    // ==================== originState ====================

    @Test
    public void originStateResolvesThroughPropagationChain() {
        CancellationToken grandparent = CancellationToken.create();
        CancellationToken parent = new CancellationToken(grandparent);
        CancellationToken child = new CancellationToken(parent);

        grandparent.timeoutCancel();

        assertThat(parent.state()).isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
        assertThat(child.state()).isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
        assertThat(child.originState()).isEqualTo(CancellationToken.State.TIMEOUT);
        assertThat(parent.originState()).isEqualTo(CancellationToken.State.TIMEOUT);
        assertThat(grandparent.originState()).isEqualTo(CancellationToken.State.TIMEOUT);
    }

    @Test
    public void originStateReportsMutualCancelAcrossPropagation() {
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        parent.cancel(true);

        assertThat(child.originState()).isEqualTo(CancellationToken.State.CANCELED);
    }

    @Test
    public void originStateStopsAtTheOriginatingToken() {
        CancellationToken origin = CancellationToken.create();
        CancellationToken child = new CancellationToken(origin);
        CancellationToken grandchild = new CancellationToken(child);

        child.cancel(true);

        assertThat(grandchild.originState()).isEqualTo(CancellationToken.State.CANCELED);
        assertThat(origin.state()).isEqualTo(CancellationToken.State.RUNNING);
        assertThat(origin.originState()).isEqualTo(CancellationToken.State.RUNNING);
    }
}
