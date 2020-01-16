package com.natural.data.analyze.spark.user.visit.session;

import com.natural.data.analyze.spark.user.visit.constant.Constants;
import com.natural.data.analyze.spark.user.visit.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SortTop10Session {

    public static void getTop10Session(
            SparkSession sparkSession,
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD,
            List<Tuple2<CategorySortKey, String>> top10Category
    ) {

        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();
        JavaPairRDD<Long, String> categoryId2SessionCountRDD = sessionId2ActionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String sessionId = tuple2._1;
                Iterator<Row> iterator = tuple2._2.iterator();
                Map<Long, Long> categoryCountMap = new HashMap<>();

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (row.get(6) != null) {
                        long categoryId = row.getLong(6);
                        Long count = categoryCountMap.get(categoryId);
                        if (count == null) {
                            count = 0L;
                        }
                        count++;
                        categoryCountMap.put(categoryId, count);
                    }
                }

                List<Tuple2<Long, String>> list = new ArrayList<>();
                for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                    long categoryId = entry.getKey();
                    long count = entry.getValue();
                    String value = sessionId + "," + count;
                    list.add(new Tuple2<>(categoryId, value));
                }
                return list.iterator();
            }
        });

        List<Tuple2<Long, Long>> top10CategoryList = new ArrayList<>();
        for(Tuple2<CategorySortKey, String> category : top10Category) {
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString
                    (category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryList.add(new Tuple2<>(categoryId, categoryId));
        }







    }
}
