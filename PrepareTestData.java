package com.bigdata;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PrepareTestData {
    private static String basePath = "./src/main/resources/FileSumInput";
    private static String filePrefix = "InputFile";
    private static String fileExtension = ".txt";
    private static int bufferSize = 1024 * 8;
    public static void main(String[] args) {
        // long numberOfLines = Long.parseLong(args[0]);
        // int numberOfFiles = Integer.parseInt(args[1]);
        long numberOfLines = 100000000;
        int numberOfFiles = 2;

        List<String> files = new ArrayList<>();

        if(!basePath.endsWith("/")) {
            basePath = basePath + "/";
        }

        for (int i = 1; i <= numberOfFiles; i++) {
            files.add(basePath + filePrefix + "_" + i + fileExtension);
        }
        System.out.println("File Names :: ");
        files.forEach(System.out :: println);

        int maxLineCount = 10000;
        Random random = new Random();
        files.forEach(file -> {
            try {
                FileOutputStream outputStream = new FileOutputStream(file);
                OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                BufferedWriter writer = new BufferedWriter(streamWriter, bufferSize);
                for (long i = 1; i <= numberOfLines; i++) {
                    String line = random.nextInt(100) + " " + random.nextInt(100) + " " + random.nextInt(100);
                    writer.write(line + "\n");
                    if(i % maxLineCount == 0) {
                        writer.flush();
                    }
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }
}
