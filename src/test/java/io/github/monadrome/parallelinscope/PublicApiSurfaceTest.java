package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PublicApiSurfaceTest {
    private static final String BASE_PACKAGE = "io.github.monadrome.parallelinscope";

    private static final Set<String> EXPECTED_PUBLIC_TYPES = new TreeSet<>(Arrays.asList(
            BASE_PACKAGE + ".CancellationToken",
            BASE_PACKAGE + ".Checkpoints",
            BASE_PACKAGE + ".CombineFunction",
            BASE_PACKAGE + ".CompletedTaskValues",
            BASE_PACKAGE + ".DeadlockDetectionListener",
            BASE_PACKAGE + ".GlobalPar",
            BASE_PACKAGE + ".GlobalParDeadlockPolicy",
            BASE_PACKAGE + ".GlobalParPurgePolicy",
            BASE_PACKAGE + ".LeanCancellationException",
            BASE_PACKAGE + ".MultiTaskOptions",
            BASE_PACKAGE + ".Par",
            BASE_PACKAGE + ".ParName",
            BASE_PACKAGE + ".SmartBlockingQueue",
            BASE_PACKAGE + ".TaskBatchResult",
            BASE_PACKAGE + ".TaskCompletion",
            BASE_PACKAGE + ".TaskGraphObservationScope",
            BASE_PACKAGE + ".TaskGroup",
            BASE_PACKAGE + ".TaskGroupDefinition",
            BASE_PACKAGE + ".TaskGroupListener",
            BASE_PACKAGE + ".TaskGroupResult",
            BASE_PACKAGE + ".TaskKey",
            BASE_PACKAGE + ".TaskListener",
            BASE_PACKAGE + ".TaskOutcome",
            BASE_PACKAGE + ".TaskType",
            BASE_PACKAGE + ".queue.DrainingBlockingQueue",
            BASE_PACKAGE + ".queue.VariableLinkedBlockingQueue"));

    @Test
    void publicTopLevelTypesMatchTheReviewedApi() throws Exception {
        Set<String> actual = topLevelClassNames().stream()
                .filter(PublicApiSurfaceTest::isPublic)
                .collect(Collectors.toCollection(TreeSet::new));

        assertThat(actual).isEqualTo(EXPECTED_PUBLIC_TYPES);
    }

    private static Set<String> topLevelClassNames() throws Exception {
        URI location = GlobalPar.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI();
        Path packageRoot = Paths.get(location).resolve(BASE_PACKAGE.replace('.', '/'));
        try (Stream<Path> paths = Files.walk(packageRoot)) {
            return paths.filter(Files::isRegularFile)
                    .map(packageRoot::relativize)
                    .map(Path::toString)
                    .filter(name -> name.endsWith(".class"))
                    .filter(name -> !name.contains("$"))
                    .filter(name -> !name.endsWith("package-info.class"))
                    .map(name -> BASE_PACKAGE + "."
                            + name.substring(0, name.length() - 6).replace('/', '.'))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }

    private static boolean isPublic(String className) {
        try {
            return Modifier.isPublic(Class.forName(className, false, GlobalPar.class.getClassLoader())
                    .getModifiers());
        } catch (ClassNotFoundException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
