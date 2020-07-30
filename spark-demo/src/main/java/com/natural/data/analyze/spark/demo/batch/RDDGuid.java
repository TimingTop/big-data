package com.natural.data.analyze.spark.demo.batch;

import org.apache.spark.Partitioner;
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

    /**
     * 初始化，
     */
    public void initData() {
        // rdd 的配置信息
        SparkConf conf = new SparkConf().setAppName("rddGuid").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        JavaRDD<String> distFile = sc.textFile("data.txt");

    }

    /**
     * transform 算子
     * @param sc
     */
    public void transformationOperation(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("data.txt");
        // 1- map : 数据过滤和处理
        Integer totalLength = lines.map(s -> s.length())
                .reduce((a, b) -> a + b);
        


        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //
        lineLengths.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> lineEachRdd = lines.mapToPair(s -> new Tuple2<>(s, 1));
        // 2- reduceByKey: 相同的key进行聚合计算
        JavaPairRDD<String, Integer> lineCountRdd = lineEachRdd.reduceByKey((a, b) -> a + b);
        // 3- groupByKey: 相同的key进行分组，不聚合计算
        // 4- groupBy: 指定列进行分组
        JavaPairRDD<String, Iterable<Integer>> lineCountRdd2 = lineEachRdd.groupByKey();
        // 5- filter: 过滤
        lineCountRdd2.filter(null);
//        lineCountRdd2.partitionBy(Partitioner.defaultPartitioner())

        // 6- join: a.key == b.key 才会组合，其他过滤掉
        lineEachRdd.join(lineCountRdd);

        // ===============      accumulator  ===========================
        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accum.add(x));

        Long value = accum.value();

        //  ================     broadcast ===============

        Broadcast<String> broadcastStr = sc.broadcast("a");

        String bValue = broadcastStr.value();


    }

    /**
     * action 算子
     * @param sc
     */
    public void actionOperation(JavaSparkContext sc) {
        JavaRDD<String> line = sc.textFile("../aaa.txt");
        JavaRDD<String> flatMapRDD = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");

                return Arrays.asList(split).iterator();
            }
        });
        // 1 collect：  返回一个数组
        // 把所有数据读到内存，数据量少时才能使用，否则会oom
        flatMapRDD.collect();
        // 2 count: 返回个数
        flatMapRDD.count();
        flatMapRDD.saveAsTextFile("aaaa/bbb/cc.txt");
        flatMapRDD.saveAsObjectFile("dd/bbb.txt");
        // 3 take: 返回前几个数据
        flatMapRDD.take(3);
        // 4 takeOrdered: 返回指定排序的前n个数据
        flatMapRDD.takeOrdered(3);
    }
}
