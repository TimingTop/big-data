package com.natural.data.analyze.spark.user.visit.task;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class UserVisitTask {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("aa");

//        SparkContext sparkContext = new SparkContext(sparkConf);

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // 把 mock 的数据 转成 表结构，然后再过滤


        // 从 sql 中查出 需要用的 数据集






    }
}
