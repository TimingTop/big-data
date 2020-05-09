package com.natural.data.analyze.spark.user.visit.product;

import com.natural.data.analyze.spark.user.visit.util.SimulateData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


/**
 *
 * 使用 spark sql 来处理
 *
 *
 *
 */
public class Top5ProductTask {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("top5Product")
                .master("local[2]")
                .getOrCreate();

        SQLContext sqlContext = sparkSession.sqlContext();

        SimulateData.simulation(sqlContext);

        Dataset<Row> clickActionDS = sqlContext.sql("select city_id, click_product_id from user_visit_action " +
                "where click_product_id is not null or click_product_id != 'NULL' " +
                "or click_product_id != 'null'");
        JavaRDD<Row> clickActionRDD = clickActionDS.javaRDD();
        JavaPairRDD<Long, Row> cityId2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(12), row);
            }
        });


        // row ==>
        // <city_id, action_row>        user_visit_action

        //  <city_id, city_row>       city_info

        // join

        // <city_id, <action_row, city_row>>

        // map   然后构建 DataSet

        //



    }
}
