package ecnu.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.Objects;

public class TransferWorkload {

    private static final Path inputWorkloadPath = Path.of("benchmark");
    private static final Path outputWorkloadPath = Path.of("benchmark-transferred");

    private static void checkStrSafe(String input) {
        for (char c : input.toCharArray()) {
            if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c == '_')) {
                continue;
            }
            throw new UnsupportedOperationException(input);
        }
    }

    private static String replaceEachSQlFileWithLowerCaseAscii(Path fileName) throws IOException {
        StringBuilder sqlContent = new StringBuilder();
        for (String line : Files.readAllLines(fileName)) {
            String[] parts = line.split("\"");
            for (int i = 0; i < parts.length; i++) {
                if (i % 2 == 1) {
                    // 替换掉所有的空格和其他字符
                    parts[i] = parts[i].replaceAll("\\s+|/|#|-|\\(|\\)|:|\\+|&|\\.|,|\\?|%|\\$", "_");
                    // 替换掉所有的重音字母
                    parts[i] = Normalizer.normalize(parts[i], Normalizer.Form.NFKD).replaceAll("\\p{M}", "");
                    parts[i] = parts[i].toLowerCase();
                    // 检查替换掉的字母是否是合法的
                    checkStrSafe(parts[i]);
                }
            }
            // 重新组装SQL
            sqlContent.append(String.join("", parts)).append(System.lineSeparator());
        }
        return sqlContent.toString();
    }

    private static String replaceDoubleWithDoublePrecision(String sqlContent) {
        sqlContent = sqlContent.replace("as double", "as double precision");
        return sqlContent.replace("AS double", "AS double precision");
    }

    private static String replaceMedianWithMin(String sqlContent) {
        return sqlContent.replace("MEDIAN", "MIN");
    }

    public static void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteFolder(file);
                }
            }
        }
        folder.delete();
    }

    public static void main(String[] args) throws IOException {
        deleteFolder(outputWorkloadPath.toFile());
        int i = 0;
        for (File benchmark : Objects.requireNonNull(inputWorkloadPath.toFile().listFiles())) {
            if (!benchmark.isDirectory()) {
                continue;
            }
            File inputQueryDir = new File(benchmark.toPath().resolve("queries").toString());
            if (!inputQueryDir.exists() || !inputQueryDir.isDirectory()) {
                throw new IOException();
            }
            Path outputBenchmarkDir = outputWorkloadPath.resolve(benchmark.getName());
            Files.createDirectories(outputBenchmarkDir);
            for (File sql : Objects.requireNonNull(inputQueryDir.listFiles())) {
                if (sql.getPath().endsWith(".sql")) {
                    String sqlContent = replaceEachSQlFileWithLowerCaseAscii(sql.toPath());
                    sqlContent = replaceDoubleWithDoublePrecision(sqlContent);
                    sqlContent = replaceMedianWithMin(sqlContent);
                    Files.write(outputBenchmarkDir.resolve(sql.getName()), sqlContent.getBytes());
                    i++;
                }
            }
        }
        System.out.printf("处理%d条Query%s", i, System.lineSeparator());
    }
}
