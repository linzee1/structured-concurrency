package demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * 架构约束测试
 *
 * <p>验证 demo 子项目符合架构约束：
 *
 * <ul>
 *   <li>不使用已移除的旧包
 *   <li>使用正确的包命名空间
 * </ul>
 */
class ArchitectureConstraintsTest {

    /** 0.2.0 在发布前收敛掉的历史包。 */
    private static final List<String> REMOVED_PACKAGES = Arrays.asList(
            "io.github.monadrome.parallelinscope.scope",
            "io.github.monadrome.parallelinscope.internal",
            "io.github.monadrome.parallelinscope.cancel",
            "io.github.monadrome.parallelinscope.context",
            "io.github.monadrome.parallelinscope.spi",
            "io.github.monadrome.parallelinscope.control");

    @Test
    void testNoRemovedPackageImports() throws IOException {
        Path sourceRoot = Paths.get("src/main/java");

        if (!Files.exists(sourceRoot)) {
            // 如果源目录不存在，跳过测试
            return;
        }

        try (Stream<Path> javaFiles =
                Files.walk(sourceRoot).filter(path -> path.toString().endsWith(".java"))) {

            List<String> violations =
                    javaFiles.flatMap(this::checkFileForRemovedImports).collect(Collectors.toList());

            assertThat(violations).as("Should not import removed packages").isEmpty();
        }
    }

    @Test
    void testUsesDemoPackageNamespace() throws IOException {
        Path sourceRoot = Paths.get("src/main/java");

        if (!Files.exists(sourceRoot)) {
            return;
        }

        try (Stream<Path> javaFiles =
                Files.walk(sourceRoot).filter(path -> path.toString().endsWith(".java"))) {

            List<String> violations = javaFiles
                    .filter(path -> !isTestFile(path))
                    .flatMap(this::checkFilePackageDeclaration)
                    .collect(Collectors.toList());

            assertThat(violations).as("Should use demo package namespace").isEmpty();
        }
    }

    private Stream<String> checkFileForRemovedImports(Path javaFile) {
        try {
            List<String> lines = Files.readAllLines(javaFile);
            String fileName = javaFile.getFileName().toString();

            return lines.stream()
                    .filter(line -> line.trim().startsWith("import "))
                    .filter(line -> REMOVED_PACKAGES.stream().anyMatch(pkg -> line.contains(pkg + ".")))
                    .map(line -> String.format("%s: removed package import: %s", fileName, line.trim()));
        } catch (IOException e) {
            return Stream.of("Error reading " + javaFile + ": " + e.getMessage());
        }
    }

    private Stream<String> checkFilePackageDeclaration(Path javaFile) {
        try {
            List<String> lines = Files.readAllLines(javaFile);
            String fileName = javaFile.getFileName().toString();

            return lines.stream()
                    .filter(line -> line.trim().startsWith("package "))
                    .filter(line -> !line.trim().startsWith("package demo"))
                    .map(line -> String.format("%s: should use demo package, found: %s", fileName, line.trim()));
        } catch (IOException e) {
            return Stream.of("Error reading " + javaFile + ": " + e.getMessage());
        }
    }

    private boolean isTestFile(Path path) {
        return path.toString().contains("/test/") || path.toString().contains("\\test\\");
    }
}
