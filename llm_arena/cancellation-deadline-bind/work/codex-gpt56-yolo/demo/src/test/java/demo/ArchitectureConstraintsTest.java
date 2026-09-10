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
 *   <li>不访问禁止的内部包
 *   <li>使用正确的包命名空间
 * </ul>
 */
class ArchitectureConstraintsTest {

    /** 禁止访问的内部包列表 */
    private static final List<String> FORBIDDEN_PACKAGES = Arrays.asList(
            "io.github.huatalk.parallelinscope.internal",
            "io.github.huatalk.parallelinscope.cancel",
            "io.github.huatalk.parallelinscope.context",
            "io.github.huatalk.parallelinscope.context.graph",
            "io.github.huatalk.parallelinscope.queue");

    /** 允许的例外类（来自禁止包的白名单） */
    private static final List<String> ALLOWED_EXCEPTIONS =
            Arrays.asList("io.github.huatalk.parallelinscope.cancel.Checkpoints");

    @Test
    void testNoForbiddenPackageImports() throws IOException {
        Path sourceRoot = Paths.get("src/main/java");

        if (!Files.exists(sourceRoot)) {
            // 如果源目录不存在，跳过测试
            return;
        }

        try (Stream<Path> javaFiles =
                Files.walk(sourceRoot).filter(path -> path.toString().endsWith(".java"))) {

            List<String> violations =
                    javaFiles.flatMap(this::checkFileForForbiddenImports).collect(Collectors.toList());

            assertThat(violations).as("Should not import forbidden packages").isEmpty();
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

    private Stream<String> checkFileForForbiddenImports(Path javaFile) {
        try {
            List<String> lines = Files.readAllLines(javaFile);
            String fileName = javaFile.getFileName().toString();

            return lines.stream()
                    .filter(line -> line.trim().startsWith("import "))
                    .filter(line -> FORBIDDEN_PACKAGES.stream().anyMatch(pkg -> line.contains(pkg)))
                    .filter(line -> ALLOWED_EXCEPTIONS.stream().noneMatch(ex -> line.contains(ex)))
                    .map(line -> String.format("%s: forbidden import: %s", fileName, line.trim()));
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
