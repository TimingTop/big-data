package com.natural.data.analyze.spark.word;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public class WordCount {

    public static void main(String[] args) {
        // config default local
        SparkConf conf = new SparkConf();
        conf.setAppName("MyWorkCount");
        // context 上下文
        SparkContext context = new SparkContext(conf);

        RDD<String> rdd = context.textFile("/aba.txt", 3);



    }
}
