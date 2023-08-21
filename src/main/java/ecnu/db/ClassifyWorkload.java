package ecnu.db;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class ClassifyWorkload {
    private static final Path transferWorkloadPath = Path.of("benchmark-transferred");
    private static final Path classifyWorkloadPath = Path.of("benchmark-classified");
    private static final Path classifyWorkloadAndOrPath = Path.of("benchmark-classified-andor");
    private static final Path classifyWorkloadIsNullPath = Path.of("benchmark-classified-isnull");
    //按照where数量分类
    private static final Path noWherePath = Path.of("benchmark-classified/noWhere");
    private static final Path oneWherePath = Path.of("benchmark-classified/oneWhere");
    private static final Path oneWhereIsNullPath = Path.of("benchmark-classified/oneWhereIsNull");
    private static final Path twoWherePath = Path.of("benchmark-classified/twoWhere");
    //按照and和or类型分类
    private static final Path andPath = Path.of("benchmark-classified-andor/and");
    private static final Path orPath = Path.of("benchmark-classified-andor/or");
    private static final Path andOrPath = Path.of("benchmark-classified-andor/and-or");
    //按照有没有isnull关键字分类
    private static final Path isNullPath = Path.of("benchmark-classified-isnull/isnull");
    private static final Path notIsNullPath = Path.of("benchmark-classified-isnull/notisnull");
    private static final Pattern queriesPattern = Pattern.compile("[0-9]+\\.sql");

    public static void main(String[] args) throws IOException {
        //classifyWhereCount();
        classifyAndOr();
        //classifyIsNull();
    }

    public static void classifyIsNull() throws IOException {
        deleteFolder(classifyWorkloadIsNullPath.toFile());
        Files.createDirectories(isNullPath);
        Files.createDirectories(notIsNullPath);
        File file = transferWorkloadPath.toFile();
        List<File> allSqlFile = searchFiles(file, ".sql");
        for (File file1 : allSqlFile) {
            String isNullPattern = "is null";
            StringBuilder s = new StringBuilder();
            for (String readAllLine : Files.readAllLines(file1.toPath())) {
                s.append(readAllLine);
                String sqlName = file1.getPath().replace("\\", "/").split("/")[1] + "_" + file1.getPath().replace("\\", "/").split("/")[2];
                if (s.toString().toLowerCase().contains(isNullPattern)) {
                    String fileName = isNullPath.toString() + "/" + sqlName;
                    File file2 = new File(fileName);
                    copyFileUsingIOStream(file1, file2);
                } else {
                    String fileName = notIsNullPath.toString() + "/" + sqlName;
                    File file2 = new File(fileName);
                    copyFileUsingIOStream(file1, file2);
                }
            }
        }
    }

    public static void classifyAndOr() throws IOException {
        deleteFolder(classifyWorkloadAndOrPath.toFile());
        Files.createDirectories(andPath);
        Files.createDirectories(orPath);
        Files.createDirectories(andOrPath);
        File file = oneWherePath.toFile();
        List<File> allSqlFile = searchFiles(file, ".sql");
        for (File file1 : allSqlFile) {
            String andPattern = " AND ";
            String orPattern = " OR ";
            StringBuilder s = new StringBuilder();
            for (String readAllLine : Files.readAllLines(file1.toPath())) {
                s.append(readAllLine);
            }
            String originS = s.toString();
            String sqlName = /*file1.getPath().replace("\\","/").split("/")[1] + "_" + */file1.getPath().replace("\\","/").split("/")[2];
            if (s.toString().contains(andPattern) && !s.toString().contains(orPattern)) {
                String fileName = andPath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
            } else if (!s.toString().contains(andPattern) && s.toString().contains(orPattern)) {
                String fileName = orPath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
            } else if (s.toString().contains(andPattern) && s.toString().contains(orPattern)) {
                String fileName = andOrPath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
            } else {
                String fileName = andPath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
                //System.out.println(originS);
            }
        }
    }

    public static void classifyWhereCount() throws IOException {
        deleteFolder(classifyWorkloadPath.toFile());
        Files.createDirectories(noWherePath);
        Files.createDirectories(oneWherePath);
        Files.createDirectories(oneWhereIsNullPath);
        Files.createDirectories(twoWherePath);
        File file = transferWorkloadPath.toFile();
        String isNullPattern = "is null";
        List<File> allSqlFile = searchFiles(file, ".sql");
        for (File file1 : allSqlFile) {
            String wherePattern = " WHERE ";
            StringBuilder s = new StringBuilder();
            for (String readAllLine : Files.readAllLines(file1.toPath())) {
                s.append(readAllLine);
            }
            String originS = s.toString();
            int count = 0;
            while (s.toString().contains(wherePattern)) {
                s = new StringBuilder(s.substring(s.indexOf(wherePattern) + 1));
                ++count;
            }
            String sqlName = file1.getPath().replace("\\", "/").split("/")[1] + "_" + file1.getPath().replace("\\", "/").split("/")[2];
            if (count == 0) {
                String fileName = noWherePath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
            } else if (count == 1) {
                if (originS.toLowerCase().contains(isNullPattern)) {
                    String fileName = oneWhereIsNullPath.toString() + "/" + sqlName;
                    File file2 = new File(fileName);
                    copyFileUsingIOStream(file1, file2);
                } else {
                    String fileName = oneWherePath.toString() + "/" + sqlName;
                    File file2 = new File(fileName);
                    copyFileUsingIOStream(file1, file2);
                }
            } else if (count > 1) {
                String fileName = twoWherePath.toString() + "/" + sqlName;
                File file2 = new File(fileName);
                copyFileUsingIOStream(file1, file2);
            } /*else {
                System.out.println(originS);
            }*/
        }
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

    public static List<File> searchFiles(File folder, String keyword) {
        List<File> result = new ArrayList<>();
        if (folder.isFile())
            result.add(folder);

        //遍历文件
        File[] fileList = folder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file.isDirectory()) {
                    return true;
                }
                if (queriesPattern.matcher(file.getName().toLowerCase()).find()) {
                    return true;
                }
                return false;
            }
        });

        if (fileList != null) {
            for (File file : fileList) {
                if (file.isFile()) {
                    //如果是文件将文件添加到result列表中
                    result.add(file);
                } else {
                    //如果是文件夹，则递归调用本方法，然后把所有的文件加到result列表中
                    result.addAll(searchFiles(file, keyword));
                }
            }
        }
        return result;
    }

    public static void copyFileUsingIOStream(File source, File dest) throws IOException {
        InputStream fis = null;
        OutputStream fos = null;
        try {
            fis = new FileInputStream(source);
            fos = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
                fos.write(buffer, 0, length);
            }
        } finally {
            fis.close();
            fos.close();
        }
    }
}
