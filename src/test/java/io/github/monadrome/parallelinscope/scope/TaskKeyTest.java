package io.github.monadrome.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TaskKeyTest {

    @Test
    void keysAreEqualWhenTheyNameTheSameMember() {
        TaskKey<String> registered = new TaskKey<String>("user") {};
        TaskKey<String> sameName = new TaskKey<String>("user") {};

        assertThat(sameName).isEqualTo(registered);
        assertThat(sameName.hashCode()).isEqualTo(registered.hashCode());
    }

    @Test
    void keysDifferingOnlyInTheTypeArgumentStayEqual() {
        TaskKey<String> registered = new TaskKey<String>("user") {};

        assertThat(new TaskKey<Object>("user") {}).isEqualTo(registered);
    }

    @Test
    void keysNamingDifferentMembersAreNotEqual() {
        TaskKey<String> user = new TaskKey<String>("user") {};

        assertThat(new TaskKey<String>("orders") {}).isNotEqualTo(user);
        assertThat(user).isNotEqualTo(null);
        assertThat(user).isNotEqualTo("user");
    }

    @Test
    void keysAreUsableInSets() {
        Set<TaskKey<?>> keys = new HashSet<>();
        keys.add(new TaskKey<String>("user") {});
        keys.add(new TaskKey<Object>("user") {});
        keys.add(new TaskKey<String>("orders") {});

        assertThat(keys).hasSize(2);
    }
}
