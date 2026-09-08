package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.*;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.github.huatalk.parallelinscope.cancel.*;
import io.github.huatalk.parallelinscope.context.*;
import io.github.huatalk.parallelinscope.context.graph.*;
import io.github.huatalk.parallelinscope.queue.*;
import io.github.huatalk.parallelinscope.scope.*;
import io.github.huatalk.parallelinscope.spi.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for Future state inspection via FutureInspector.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class FutureInspectorTest {

    @Test
    public void testState_success() {
        ListenableFuture<String> future = Futures.immediateFuture("ok");
        assertThat(FutureInspector.state(future)).isEqualTo(TaskOutcome.SUCCESS);
    }

    @Test
    public void testState_canceled() {
        ListenableFuture<String> future = Futures.immediateCancelledFuture();
        assertThat(FutureInspector.state(future)).isEqualTo(TaskOutcome.MEMBER_CANCELED);
    }

    @Test
    public void testState_failed() {
        ListenableFuture<String> future = Futures.immediateFailedFuture(new RuntimeException("fail"));
        assertThat(FutureInspector.state(future)).isEqualTo(TaskOutcome.USER_FAILURE);
    }

    @Test
    public void testState_running() {
        SettableFuture<String> future = SettableFuture.create();
        assertThat(FutureInspector.state(future)).isEqualTo(TaskOutcome.RUNNING);
    }

    @Test
    public void hintFutureReportsSubmissionFailureInsteadOfUserFailure() {
        ExecutionPhaseHintFuture<String> rejected =
                ExecutionPhaseHintFuture.createDeferred(() -> "never", phase -> {});
        rejected.submitPrepared(command -> {
            throw new java.util.concurrent.RejectedExecutionException("full");
        }, false);
        assertThat(FutureInspector.state(rejected)).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
    }

    @Test
    public void hintFutureReportsUserFailureForCallableThrow() {
        ExecutionPhaseHintFuture<String> failed = ExecutionPhaseHintFuture.create(
                () -> {
                    throw new IllegalStateException("boom");
                },
                phase -> {});
        failed.run();
        assertThat(FutureInspector.state(failed)).isEqualTo(TaskOutcome.USER_FAILURE);
    }

    @Test
    public void hintFutureReportsMemberCanceledOnCancel() {
        ExecutionPhaseHintFuture<String> canceled =
                ExecutionPhaseHintFuture.createDeferred(() -> "never", phase -> {});
        canceled.cancel(true);
        assertThat(FutureInspector.state(canceled)).isEqualTo(TaskOutcome.MEMBER_CANCELED);
    }

    @Test
    public void testExceptionNow_failed() {
        RuntimeException expected = new RuntimeException("fail");
        ListenableFuture<String> future = Futures.immediateFailedFuture(expected);
        Throwable actual = FutureInspector.exceptionNow(future);
        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void testExceptionNow_success() {
        ListenableFuture<String> future = Futures.immediateFuture("ok");
        assertThatThrownBy(() -> FutureInspector.exceptionNow(future)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testExceptionNow_pendingAndCanceledAreRejected() {
        SettableFuture<String> pending = SettableFuture.create();
        assertThatThrownBy(() -> FutureInspector.exceptionNow(pending))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not completed");

        ListenableFuture<String> canceled = Futures.immediateCancelledFuture();
        assertThatThrownBy(() -> FutureInspector.exceptionNow(canceled))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("canceled");
    }

    @Test
    public void interruptedFutureInspectionRestoresInterruptStatus() {
        Future<Object> interrupted = new Future<Object>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public Object get() throws InterruptedException {
                throw new InterruptedException("test");
            }

            @Override
            public Object get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException("test");
            }
        };
        try {
            Thread.currentThread().interrupt();
            assertThat(FutureInspector.state(interrupted)).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();

            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> FutureInspector.exceptionNow(interrupted))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
