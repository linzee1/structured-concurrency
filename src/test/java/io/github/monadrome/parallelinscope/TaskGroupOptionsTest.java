package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Contract pins for {@link TaskGroupOptions}: the group identity, its deadline, and its convergence
 * listener snapshot.
 */
class TaskGroupOptionsTest {

    @Test
    void exposesOnlyTheSurfaceAGroupReads() {
        Set<String> names = Arrays.stream(TaskGroupOptions.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> !method.isSynthetic())
                .map(Method::getName)
                .collect(Collectors.toCollection(TreeSet::new));

        // A group is not a task execution: no parallelism, no task type, no enqueue policy.
        assertThat(names)
                .isEqualTo(new TreeSet<>(Arrays.asList("inheritTimeout", "listener", "listeners", "name", "timeout")));
    }

    @Test
    void nameIsValidatedOnceAndPreservedVerbatim() {
        String name = "account-page";

        assertThat(TaskGroupOptions.inheritTimeout(name).name()).isEqualTo(name);
        assertThatThrownBy(() -> TaskGroupOptions.inheritTimeout(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> TaskGroupOptions.inheritTimeout("   ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void timeoutFactoriesAreMutuallyExclusiveByConstruction() {
        assertThat(TaskGroupOptions.inheritTimeout("page").timeout()).isEmpty();
        assertThat(TaskGroupOptions.timeout("page", Duration.ofSeconds(3)).timeout())
                .contains(Duration.ofSeconds(3));

        assertThatThrownBy(() -> TaskGroupOptions.timeout("page", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskGroupOptions.timeout("page", Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskGroupOptions.timeout("page", null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void listenersAreAnImmutableSnapshotThatKeepsTheOriginalUntouched() {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        TaskGroupListener one = event -> first.incrementAndGet();
        TaskGroupListener two = event -> second.incrementAndGet();

        TaskGroupOptions plain = TaskGroupOptions.inheritTimeout("page");
        TaskGroupOptions withOne = plain.listener(one);
        TaskGroupOptions withBoth = withOne.listener(two);

        assertThat(plain.listeners()).isEmpty();
        assertThat(withOne.listeners()).containsExactly(one);
        assertThat(withBoth.listeners()).containsExactly(one, two);
        assertThat(withBoth.name()).isEqualTo("page");
        assertThatThrownBy(() -> withBoth.listeners().add(one)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> plain.listener(null)).isInstanceOf(NullPointerException.class);
    }
}
