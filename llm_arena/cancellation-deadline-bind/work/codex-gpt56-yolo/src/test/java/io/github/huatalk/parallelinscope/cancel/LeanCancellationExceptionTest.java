package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies the zero-overhead cancellation contract: a lean cancellation exception must not retain a
 * stack trace.
 */
class LeanCancellationExceptionTest {

    @Test
    void constructor_producesEmptyStackTrace() {
        LeanCancellationException exception = new LeanCancellationException("cancel");
        assertThat(exception.getMessage()).isEqualTo("cancel");
        assertThat(exception.getStackTrace()).isEmpty();
    }

    @Test
    void fillInStackTrace_returnsSelfWithoutTracing() {
        LeanCancellationException exception = new LeanCancellationException("cancel");
        assertThat(exception.fillInStackTrace()).isSameAs(exception);
        assertThat(exception.getStackTrace()).isEmpty();
    }

    @Test
    void thrownException_remainsCancellationException() {
        Throwable thrown = new LeanCancellationException("cancel");
        assertThat(thrown).isInstanceOf(java.util.concurrent.CancellationException.class);
    }
}
