package com.natural.data.analyze.spark.user.visit.task;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DemoTask {

    public static void main(String[] args) {
        rddDemo1();
    }

    /**
     * 普通的 spark 对象，
     */
    public static void rddDemo1() {
        // initializing spark
        SparkConf conf = new SparkConf()
                .setAppName("name one")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = new ArrayList<>(2000);
        for (int i = 0; i< 2000; i++) {
            list.add(i);
        }

        long count = sc.parallelize(list)
                .filter(i -> {
                    double x = Math.random();
                    double y = Math.random();
                    return x * x + y * y < 1;
                }).count();

        System.out.println("Pi is : " + 4.0 * count/2000);
    }




    public static void insertDataToFile() {
        BufferedWriter bufferedWriter = null;
        try {
            File file = new File("data/error.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            bufferedWriter = new BufferedWriter(new FileWriter(file));
            Random  random = new Random();
            String[] keyStr = new String[]{"error", "mysql", "apache", "kafka"};
            for (int i = 0; i< 10000; i++) {
                bufferedWriter.write("content " + i + ", " + keyStr[random.nextInt(keyStr.length)]);
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public static void dataFrameDemo1() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("aaaa")
                .config(conf)
                .getOrCreate();

        JavaRDD<String> textFile = sparkSession.read().textFile("data/error.txt").toJavaRDD();

        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rowRDD, schema);

        Dataset<Row> errors = df.filter(functions.col("line").like("%error%"));

        System.out.println(errors.count());
    }

    /**
     * office demo  , 全部都是使用 sparkSession
     *
     *
     */
    public static void dataCache() {
        String logFile = "/afaf.md";
        SparkSession sparkSession = SparkSession.builder().appName("simple application").getOrCreate();
        Dataset<String> logData = sparkSession.read().textFile(logFile).cache();


        logData.filter((FilterFunction<String>) s -> s.contains("a")).count();

        sparkSession.stop();
    }

    /*
       RDD  programming guide


     */

    public static void rddDemo() {
        // 初始化
        SparkConf conf = new SparkConf().setAppName("aaa").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> distData = sc.parallelize(data);
        Integer reduce = distData.reduce((a, b) -> a + b);

        sc.textFile("");
        distData.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        // 只读 数据
        Broadcast<int[]> broadcast = sc.broadcast(new int[] {1, 2, 3});

        broadcast.value();
        // 用来累计数据·
        LongAccumulator longAccumulator = sc.sc().longAccumulator();
        longAccumulator.add(1L);
        // 注册自定义的 的 accumulator
        sc.sc().register(null, "aaa");


    }

}

