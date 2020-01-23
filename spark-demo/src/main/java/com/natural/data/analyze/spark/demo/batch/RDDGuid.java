package com.natural.data.analyze.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDGuid {

    public void initData() {
        // rdd 的配置信息
        SparkConf conf = new SparkConf().setAppName("rddGuid").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        JavaRDD<String> distFile = sc.textFile("data.txt");
    }

    public void transformationOperation(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("data.txt");
        Integer totalLength = lines.map(s -> s.length())
                .reduce((a, b) -> a + b);

        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //
        lineLengths.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> lineEachRdd = lines.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> lineCountRdd = lineEachRdd.reduceByKey((a, b) -> a + b);

        lineEachRdd.join(lineCountRdd);

        // ===============      accumulator  ===========================
        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accum.add(x));

        Long value = accum.value();

        //  ================     broadcast ===============

        Broadcast<String> broadcastStr = sc.broadcast("a");

        String bValue = broadcastStr.value();


    }

    public void actionOperation(JavaSparkContext sc) {
        JavaRDD<String> line = sc.textFile("../aaa.txt");
        JavaRDD<String> flatMapRDD = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");

                return Arrays.asList(split).iterator();
            }
        });
        flatMapRDD.collect();
        flatMapRDD.count();
        flatMapRDD.saveAsTextFile("aaaa/bbb/cc.txt");
        flatMapRDD.saveAsObjectFile("dd/bbb.txt");
        flatMapRDD.take(3);




    }
}
