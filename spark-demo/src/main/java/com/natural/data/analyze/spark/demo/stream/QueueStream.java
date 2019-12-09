package com.natural.data.analyze.spark.demo.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueueStream {


    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sparkconf");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        List<Integer> list = Stream.iterate(0, x -> x + 1)
                .limit(1000)
                .collect(Collectors.toList());


        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();

        Stream.iterate(0, x -> x + 1)
                .limit(30)
                .forEach(x -> {
                    rddQueue.add(jsc.sparkContext().parallelize(list));
                });

        JavaDStream<Integer> inputStream = jsc.queueStream(rddQueue);

        JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(x -> new Tuple2<>(x % 10, 1)).reduceByKey((x1, x2) -> x1 + x2);

        mappedStream.print();

        jsc.start();
        jsc.awaitTermination();


    }
}
