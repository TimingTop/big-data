package com.natural.data.analyze.spark.demo.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class SocketStream {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("socket stream").setMaster("local");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

//        JavaReceiverInputDStream<String> lines = ssc.socketStream("localhost", 8899, StorageLevels.MEMORY_AND_DISK_SER);


        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 8899, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((x1, x2) -> x1 + x2);

        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }

}
