package ecnu.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static String replaceLocateWithLike(String sqlContent) {
        for (int i = 3; i < 8; i++) {
            sqlContent = sqlContent.replace("locate('THE BISHOPS AVENUE',realestate2_" + i + ".street)>0",
                    "realestate2_" + i + ".street like '%THE BISHOPS AVENUE%'");
        }

        return sqlContent;
    }

    private static String replaceDoublePrecision(String sqlContent) {
        int index = sqlContent.toLowerCase().indexOf("where");
        if (index >= 0) {
            Pattern pattern = Pattern.compile("cast\\('-?\\d+\\.\\d+(00000|99999)\\d+' as double precision\\)");
            Matcher matcher = pattern.matcher(sqlContent);
            while (matcher.find()) {
                String doubleNumber = matcher.group().split("'")[1];
                sqlContent = sqlContent.replace(doubleNumber, String.valueOf(Double.parseDouble(doubleNumber)));
            }
        }
        return sqlContent;
    }

    private static String replaceEqualDate(String sqlContent) {
        Pattern pattern = Pattern.compile("CAST\\(EXTRACT\\(YEAR FROM [0-9a-z._]+\\) AS BIGINT\\) = \\d+");
        Matcher matcher = pattern.matcher(sqlContent);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            String[] allMatches = dateCompute.split(" ");
            String columnName = allMatches[2].substring(0, allMatches[2].length() - 1);
            sqlContent = sqlContent.replace(dateCompute,
                    columnName + " >= '" + allMatches[allMatches.length - 1] + "-01-01' and "
                            + columnName + " <= '" + allMatches[allMatches.length - 1] + "-12-31'");
        }
        return sqlContent;
    }

    private static String replaceIsNullDate(String sqlContent) {
        Pattern pattern = Pattern.compile("CAST\\(EXTRACT\\(YEAR FROM [0-9a-z._]+\\) AS BIGINT\\) IS NULL");
        Matcher matcher = pattern.matcher(sqlContent);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            String[] allMatches = dateCompute.split(" ");
            String columnName = allMatches[2].substring(0, allMatches[2].length() - 1);
            sqlContent = sqlContent.replace(dateCompute, columnName + " IS NULL ");
        }
        return sqlContent;
    }

    private static String replaceBoolWithInt(String sqlContent) {
        return sqlContent.replace("NOT realestate2_7.calculation_222787466257584130",
                "cast(realestate2_7.calculation_222787466257584130 as int) = 0");
    }

    private static String replaceBetDate(String sqlContent) {
        Pattern pattern = Pattern.compile("\\(CAST\\(EXTRACT\\(YEAR FROM [0-9a-z._]+\\) AS BIGINT\\) >= \\d+\\) " +
                "AND \\(CAST\\(EXTRACT\\(YEAR FROM [0-9a-z._]+\\) AS BIGINT\\) <= \\d+\\)");
        Matcher matcher = pattern.matcher(sqlContent);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            String[] betPredicates = dateCompute.split(" AND ");
            String[] firstMatches = betPredicates[0].split(" ");
            String[] secondMatches = betPredicates[1].split(" ");
            String firstColumnName = firstMatches[2].substring(0, firstMatches[2].length() - 1);
            String secondColumnName = secondMatches[2].substring(0, secondMatches[2].length() - 1);
            if (!firstColumnName.equals(secondColumnName)) {
                throw new UnsupportedOperationException();
            }
            sqlContent = sqlContent.replace(dateCompute,
                    firstColumnName + " >= '" + firstMatches[firstMatches.length - 1].replace(")", "") + "-01-01' and "
                            + firstColumnName + " <= '" + secondMatches[secondMatches.length - 1].replace(")", "") + "-12-31'");
        }
        return sqlContent;
    }


    private static String replaceInDate(String sqlContent) {
        Pattern pattern = Pattern.compile("CAST\\(EXTRACT\\(YEAR FROM [0-9a-z._]+\\) AS BIGINT\\) IN \\(\\d+(, \\d+)+\\)");
        Matcher matcher = pattern.matcher(sqlContent);
        while (matcher.find()) {
            String dateCompute = matcher.group();
            String[] predicateAndInParameters = dateCompute.split(" IN ");
            String[] allMatches = predicateAndInParameters[0].split(" ");
            String columnName = allMatches[2].substring(0, allMatches[2].length() - 1);
            String[] parametersMatches = predicateAndInParameters[1].substring(1, predicateAndInParameters[1].length() - 1).split(",");
            int start = Integer.parseInt(parametersMatches[0].trim());
            int old = start;
            for (int i = 1; i < parametersMatches.length; i++) {
                int current = Integer.parseInt(parametersMatches[i].trim());
                if (current != old + 1) {
                    throw new UnsupportedOperationException();
                }
                old = current;
            }
            sqlContent = sqlContent.replace(dateCompute, columnName + " >= '" + start + "-01-01' and " + columnName + " <= '" + old + "-12-31'");
        }
        return sqlContent;
    }

    private static String replaceWhereExtractWithBetweenAnd(String sqlContent) {
        int index = sqlContent.toLowerCase().indexOf("where");
        if (index >= 0) {
            String whereCondition = sqlContent.substring(index + 5);
            whereCondition = replaceExtraDateCheck(whereCondition);
            whereCondition = replaceEqualDate(whereCondition);
            whereCondition = replaceInDate(whereCondition);
            whereCondition = replaceIsNullDate(whereCondition);
            whereCondition = replaceBetDate(whereCondition);
            sqlContent = sqlContent.substring(0, index);
            if (!whereCondition.toLowerCase().contains("extract")) {
                sqlContent += " WHERE " + whereCondition;
            }
        }
        return sqlContent;
    }

    private static String replaceExtraDateCheck(String whereCondition) {
        whereCondition = whereCondition.replace(" OR (CAST(EXTRACT(MONTH FROM generico_5.fecha) AS BIGINT) IS NULL)", "");
        whereCondition = whereCondition.replace("(CAST(EXTRACT(MONTH FROM generico_5.fecha) AS BIGINT) IS NULL) OR ", "");
        whereCondition = whereCondition.replace("(CAST(EXTRACT(MONTH FROM generico_5.fecha) AS BIGINT) >= 1) AND ", "");
        whereCondition = whereCondition.replace("(CAST(EXTRACT(MONTH FROM motos_1.fecha) AS BIGINT) >= 1) AND", "");
        whereCondition = whereCondition.replace("(CAST(EXTRACT(MONTH FROM motos_2.fecha) AS BIGINT) >= 1) AND", "");
        return whereCondition;
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


    private static String replaceHavingSum1(String sqlContent) {
        return sqlContent.replace("HAVING ((SUM(1) >= 30) AND (SUM(1) <= 100000))", "HAVING ((COUNT(1) >= 30) AND (COUNT(1) <= 100000))");
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
                    sqlContent = replaceLocateWithLike(sqlContent);
                    sqlContent = replaceWhereExtractWithBetweenAnd(sqlContent);
                    sqlContent = replaceHavingSum1(sqlContent);
                    sqlContent = replaceBoolWithInt(sqlContent);
                    sqlContent = replaceDoublePrecision(sqlContent);
                    Files.write(outputBenchmarkDir.resolve(sql.getName()), sqlContent.getBytes());
                    i++;
                }
            }
        }
        System.out.printf("处理%d条Query%s", i, System.lineSeparator());
    }
}
