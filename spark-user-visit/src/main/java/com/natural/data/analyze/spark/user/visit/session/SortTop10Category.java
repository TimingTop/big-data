package com.natural.data.analyze.spark.user.visit.session;

// 获取前十 的类别

import com.natural.data.analyze.spark.user.visit.constant.Constants;
import com.natural.data.analyze.spark.user.visit.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SortTop10Category {
    // (session_id, aggrInfo)    (session_id, row)
    public static void getTop10Category(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2actionRDD
    ) {

        JavaPairRDD<String, Row> sessionId2detailRDD = sessionId2AggrInfoRDD.join(sessionId2actionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<>(tuple._1, tuple._2._2);
                    }
                });

        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }
                String orderCategoryId = row.getString(8);
                if (orderCategoryId != null) {
                    list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                }
                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    list.add(new Tuple2<>(Long.valueOf(payCategoryIds), Long.valueOf(payCategoryIds)));
                }
                return list.iterator();
            }
        });


        categoryIdRDD = categoryIdRDD.distinct();

        JavaPairRDD<Long, Long> clickCategoryId2countRDD = getClickCategoryId2CountRDD(sessionId2detailRDD);
        JavaPairRDD<Long, Long> orderCategoryId2countRDD = getOrderCategoryId2CountRDD(sessionId2detailRDD);
        JavaPairRDD<Long, Long> payCategoryId2countRDD = getPayCategoryId2CountRDD(sessionId2detailRDD);

        JavaPairRDD<Long, String> categoryId2countRDD = joinCategoryAndData(
                categoryIdRDD, clickCategoryId2countRDD, orderCategoryId2countRDD,
                payCategoryId2countRDD);

        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryId2countRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2) throws Exception {
                String countInfo = tuple2._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString
                        (countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                        countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(sortKey, countInfo);
            }
        });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);

        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String countInfo = tuple2._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            System.out.println("categoryId = " + categoryid +
                    ", clickCount = " + clickCount +
                    ", orderCount = " + orderCount +
                    ", payCount = " + payCount);
        }


    }

    public static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD){
        JavaPairRDD<Long, String> tmpJoinRDD =
                // (categoryid, (categoryid, count))
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).mapToPair(
                        new PairFunction<Tuple2<Long,Tuple2<Long, Optional<Long>>>, Long,String >() {
                            @Override
                            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2)
                                    throws Exception {
                                long categoryid = tuple2._1;
                                Optional<Long> option = tuple2._2._2;
                                Long clickCount = 0L;
                                if(option.isPresent())
                                {
                                    clickCount = option.get();
                                }
                                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                                        Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                                return new Tuple2<Long, String>(categoryid, value);
                            }


                        });

        // (categoryid, "cagd:1|click:count")

        tmpJoinRDD = tmpJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long,Tuple2<String, Optional<Long>>>, Long, String>(){

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
                        Long categoryid = tuple2._1;
                        String value = tuple2._2._1;
                        Optional<Long> optional =tuple2._2._2;
                        long orderCount = 0L;

                        if(optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                        return new Tuple2<Long, String>(categoryid, value) ;
                    }
                });


        tmpJoinRDD = tmpJoinRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if(optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });
        return tmpJoinRDD;
    }

    public static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionId2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                return row.get(6) != null ? true : false;
            }
        });

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple2) throws Exception {
                long clickCategoryId = tuple2._2.getLong(6);
                return new Tuple2<>(clickCategoryId, 1L);
            }
        });

        JavaPairRDD<Long, Long> clickCategoryId2countRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2countRDD;
    }

    public static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD){
        JavaPairRDD<String, Row> orderActionRDD = sessionId2detailRDD.filter(
                new Function<Tuple2<String,Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                        Row row = tuple2._2;
                        return row.getString(8)!=null ? true:false;
                    }
                });
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                        Row row = tuple2._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for(String orderCategoryId:orderCategoryIdsSplited)
                        {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),1L));
                        }
                        return list.iterator();

                    }
                });
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1+v2;
                    }
                });
        return orderCategoryId2CountRDD;
    }

    public static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2detailRDD.filter(
                new Function<Tuple2<String,Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for(String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list.iterator();
                    }

                });

        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });
        return payCategoryId2CountRDD;
    }
}
