package com.natural.data.analyze.spark.user.visit.session;

import com.natural.data.analyze.spark.user.visit.util.SimulateData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;

/**
 *
 * https://spark.apache.org/docs/latest/rdd-programming-guide.html
 *
 * driver
 *
 * 使用 RDD 计算，就是 spark core
 *
 * 若果需要访问 hdfs ，引入   hadoop-client
 */
public class UserSessionMain {

    public static void main(String[] args) {
//        String logFIle = "source/bab.log";
//        SparkSession spark = SparkSession.builder()
//                .appName("Simple Application")
//                .getOrCreate();
//        Dataset<String> logData = spark.read().textFile(logFIle).cache();
//        spark.stop();

//        JavaRDD<Integer> distData = sc.parallelize(Arrays.asList(1, 2, 3));
//        sc.textFile("hdfs://afda.txt");
//        sc.textFile("/my/directory");
//        sc.textFile("my/directory/*.log");
//        //sc.newAPIHadoopFile()
//
//        distData.saveAsObjectFile("source");
//        distData.saveAsTextFile("source");
//
//        distData.persist(StorageLevel.MEMORY_ONLY());
//        Broadcast<int[]> broadcast = sc.broadcast(new int[]{1, 2, 3});
//        broadcast.value();
//
//        LongAccumulator accum = sc.sc().longAccumulator();
//        sc.sc().register(accum, "hehe");
//        accum.value();

        // initializing spark
        SparkConf conf = new SparkConf()
                .setAppName("name one")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        String taskParam = "";


        SparkSession spark = SparkSession
                .builder()
                .appName("testone")
                .master("local[2]")
                .getOrCreate();

        SimulateData.simulation(spark.sqlContext());

        Dataset<Row> productResult = spark.sql("select * from product_info");
//        productResult.show(1);

        JavaRDD<Row> rowJavaRDD = productResult.javaRDD();


        JavaRDD<String> text = sc.textFile("data/people.txt");
        JavaPairRDD<String, Integer> count = text.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        count.saveAsTextFile("data/result.txt");




    }
}
