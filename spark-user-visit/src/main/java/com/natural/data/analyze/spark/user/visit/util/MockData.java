package com.natural.data.analyze.spark.user.visit.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MockData {

    public static void main(String[] args) {

    }

    public static void mock() throws IOException {

        File file = new File("data/people.txt");
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fileWriter = new FileWriter(file, false);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        bufferedWriter.write("aa");

        bufferedWriter.newLine();


        bufferedWriter.close();



    }
}
