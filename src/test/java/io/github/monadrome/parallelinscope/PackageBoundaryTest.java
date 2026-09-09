package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PackageBoundaryTest {
    private static final String ROOT = "io.github.monadrome.parallelinscope";
    private static final Pattern PACKAGE = Pattern.compile("(?m)^package\\s+([\\w.]+);");
    private static final Pattern PROJECT_IMPORT =
            Pattern.compile("(?m)^import\\s+" + ROOT.replace(".", "\\.") + "([\\w.]*);");

    @Test
    void mainSourcesUseOnlyTheRootAndIndependentQueuePackages() throws IOException {
        Set<String> packages = new TreeSet<>();
        for (Path source : mainSources()) {
            Matcher declaration =
                    PACKAGE.matcher(new String(Files.readAllBytes(source), java.nio.charset.StandardCharsets.UTF_8));
            assertThat(declaration.find())
                    .as("package declaration in %s", source)
                    .isTrue();
            packages.add(declaration.group(1));
        }

        assertThat(packages).containsExactly(ROOT, ROOT + ".queue");
    }

    @Test
    void independentQueuePackageDoesNotDependOnTheExecutionKernel() throws IOException {
        Path queue = Paths.get("src/main/java").resolve(ROOT.replace('.', '/')).resolve("queue");
        try (Stream<Path> paths = Files.walk(queue)) {
            for (Path source :
                    paths.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList())) {
                String content = new String(Files.readAllBytes(source), java.nio.charset.StandardCharsets.UTF_8);
                Matcher imports = PROJECT_IMPORT.matcher(content);
                while (imports.find()) {
                    assertThat(imports.group(1))
                            .as("project import in %s", source)
                            .startsWith(".queue.");
                }
            }
        }
    }

    private static Set<Path> mainSources() throws IOException {
        Path root = Paths.get("src/main/java").resolve(ROOT.replace('.', '/'));
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(path -> path.toString().endsWith(".java"))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
