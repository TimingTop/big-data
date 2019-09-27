package com.natural.data.analyze.spark.customer.analyze;

import com.natural.data.analyze.core.config.ConfigurationManager;
import com.natural.data.analyze.core.constant.Constants;
import com.natural.data.analyze.core.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class CustomerVisitSessionAnalyzeMain {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(Constants.SPARK_APP_NAME);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = Context.getSQLContext(sc.sc());




    }
}
