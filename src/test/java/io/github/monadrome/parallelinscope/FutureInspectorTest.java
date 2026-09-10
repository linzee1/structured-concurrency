package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.*;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.github.monadrome.parallelinscope.queue.*;
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
    public void testOutcome_success() {
        ListenableFuture<String> future = Futures.immediateFuture("ok");
        assertThat(FutureInspector.outcome(future)).isEqualTo(TaskOutcome.SUCCESS);
    }

    @Test
    public void testOutcome_canceled() {
        ListenableFuture<String> future = Futures.immediateCancelledFuture();
        assertThat(FutureInspector.outcome(future)).isEqualTo(TaskOutcome.MEMBER_CANCELED);
    }

    @Test
    public void testOutcome_failed() {
        ListenableFuture<String> future = Futures.immediateFailedFuture(new RuntimeException("fail"));
        assertThat(FutureInspector.outcome(future)).isEqualTo(TaskOutcome.USER_FAILURE);
    }

    @Test
    public void testOutcome_running() {
        SettableFuture<String> future = SettableFuture.create();
        assertThat(FutureInspector.outcome(future)).isEqualTo(TaskOutcome.RUNNING);
    }

    @Test
    public void taskFutureSuppliesTheAttributionABareFutureCannot() {
        CancellationToken token = new CancellationToken();
        token.timeoutCancel();
        ExecutionPhaseHintFuture<String> canceled = ExecutionPhaseHintFuture.create(() -> "never", phase -> {});
        canceled.cancel(true);

        // The same future under inspection: only the delivered TaskFuture view can tell that the
        // cancellation came from the token's deadline.
        assertThat(FutureInspector.outcome(canceled)).isEqualTo(TaskOutcome.MEMBER_CANCELED);
        assertThat(FutureInspector.outcome(Task.of("task", token, canceled))).isEqualTo(TaskOutcome.TIMEOUT);
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
            assertThat(FutureInspector.outcome(interrupted)).isEqualTo(TaskOutcome.USER_FAILURE);
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
