package com.natural.data.analyze.spark.demo.batch;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * RDD   最原始的 数据结构
 *
 *
 *
 *       xx.txt                    ------  sc.textFile
 *       (strs)                  ------ flatMap
 *       (str)                  ------ mapToPair
 *       (str, count)           ------- reduceByKey
 *       (str, [count1, count2, count3])           (str, counts)
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
public class WordCountRDD {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("JavaWordCount")
                .master("local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("data/word1.txt").javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        spark.stop();

    }
}
