package com.natural.data.analyze.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.internal.config.SparkConfigProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 *
 *  dataFrame  是一种有声明 表结构的 RDD， 是RDD 的一种封装，
 *
 *  简单说 就是 吧 RDD 封装成  表结构
 *
 *
 */

public class ErrorDataFrame {

    public static void main(String[] args) {

//        SparkConf conf = new SparkConf().setMaster("local").setAppName("errorCount");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("errorCount")
                .getOrCreate();

        JavaRDD<String> textFile = spark.read().textFile("data/error.txt").javaRDD();

        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df= spark.sqlContext().createDataFrame(rowRDD, schema);


        Dataset<Row> errors = df.filter(col("line").like("%error%"));

        System.out.println(errors.count());

        Dataset<Row> mysql = df.filter(col("line").like("%mysql%"));

        System.out.println(mysql.count());

        // Rigister the  dataframe as a SQL temporary view
        df.createOrReplaceTempView("error");


        Dataset<Row> sqlDF = spark.sql("select * from error");
        sqlDF.show(10);


    }
}
