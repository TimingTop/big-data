package com.natural.data.analyze.spark.user.visit.task;

import com.natural.data.analyze.spark.user.visit.constant.Constants;
import com.natural.data.analyze.spark.user.visit.session.SessionAggrStatAccumulator;
import com.natural.data.analyze.spark.user.visit.util.DateUtils;
import com.natural.data.analyze.spark.user.visit.util.SimulateData;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 *
 * https://spark.apache.org/docs/latest/rdd-programming-guide.html
 *
 * driver
 *
 * 使用 RDD 计算，就是 spark core
 *
 * 若果需要访问 hdfs ，引入   hadoop-client
 */
public class UserSessionTask {

    public static void main(String[] args) {
//        String logFIle = "source/bab.log";
//        SparkSession spark = SparkSession.builder()
//                .appName("Simple Application")
//                .getOrCreate();
//        Dataset<String> logData = spark.read().textFile(logFIle).cache();
//        spark.stop();

//        JavaRDD<Integer> distData = sc.parallelize(Arrays.asList(1, 2, 3));
//        sc.textFile("hdfs://afda.txt");
//        sc.textFile("/my/directory");
//        sc.textFile("my/directory/*.log");
//        //sc.newAPIHadoopFile()
//
//        distData.saveAsObjectFile("source");
//        distData.saveAsTextFile("source");
//
//        distData.persist(StorageLevel.MEMORY_ONLY());
//        Broadcast<int[]> broadcast = sc.broadcast(new int[]{1, 2, 3});
//        broadcast.value();
//
//        LongAccumulator accum = sc.sc().longAccumulator();
//        sc.sc().register(accum, "hehe");
//        accum.value();

        // initializing spark
//        SparkConf conf = new SparkConf()
//                .setAppName("name one")
//                .setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        String taskParam = "";
//
//        JavaRDD<String> text = sc.textFile("data/people.txt");
//        JavaPairRDD<String, Integer> count = text.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b);

//        count.saveAsTextFile("data/result.txt");


        SparkSession spark = SparkSession
                .builder()
                .appName("testone")
                .master("local[2]")
                .getOrCreate();

        /*****************************************
         * 表结构
         * ========== user_visit_action ===============
         *
         * 0 date
         * 1 user_id
         * 2 session_id
         * 3 page_id
         * 4 action_time
         * 5 search_keyword
         * 6 click_category_id
         * 7 click_product_id
         * 8 order_category_ids
         * 9 order_product_ids
         * 10 pay_category_ids
         * 11 pay_product_ids
         * 12 city_id
         */
        // 模拟数据，创建一个内存表
        SimulateData.simulation(spark.sqlContext());
        // 查数据
        Dataset<Row> productResult = spark.sql("select * from product_info");
//        productResult.show(1);

        JavaRDD<Row> actionRDD = productResult.javaRDD();

        // （session_id, Row）
        JavaPairRDD<String, Row> session2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        // 持久化数据到 内存
        session2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        // (session_id, [Row1, Row2, Row3])
        JavaPairRDD<String, Iterable<Row>> session2actionRDDS = session2actionRDD.groupByKey();

        JavaPairRDD<Long, String> userId2aggrInfoRDD = session2actionRDDS.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;

                Iterator<Row> iterator = tuple._2.iterator();

                Long useId = null;
                // session  time
                Date startTime = null;
                Date endTime = null;

                int stepLength = 0;
                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (useId == null) {
                        useId = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }

                    if (clickCategoryId != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }

                    Date actionTime = DateUtils.parseTime(row.getString(4));

                    if (actionTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }

                    stepLength++;

                }

                String searchKeywords = com.natural.data.analyze.spark.user.visit.util.StringUtils
                        .trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = com.natural.data.analyze.spark.user.visit.util.StringUtils
                        .trimComma(clickCategoryIdsBuffer.toString());

                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                String aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                return new Tuple2<>(useId, aggrInfo);
            }
        });

        //    =================   查 用户
        JavaRDD<Row> userInfoRDD = spark.sql("select * from user_info").javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        // ========================= end

        JavaPairRDD<Long, Tuple2<String, Row>> userId2fullInfoRDD = userId2aggrInfoRDD.join(userId2InfoRDD);
        // (sessionId, fullInfoRow)
        JavaPairRDD<String, String> sessionId2fullAggrInfoRDD = userId2fullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String aggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionId = com.natural.data.analyze.spark.user.visit.util.StringUtils
                        .getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = aggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;


                return new Tuple2<>(sessionId, fullAggrInfo);
            }
        });

        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();

        spark.sparkContext().register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");

        // 过滤一部分的数据，然后还要统计一下，需要用到的 session 数据
        JavaPairRDD<String, String> filterSessionId2aggrInfoRDD = sessionId2fullAggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                long visitLength = Long.valueOf(com.natural.data.analyze.spark.user.visit.util.StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(com.natural.data.analyze.spark.user.visit.util.StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);

                return true;
            }

            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }

        });

        filterSessionId2aggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        // 输出 每个 统计的数据
        String sessionStr = sessionAggrStatAccumulator.value();

        long session_count = Long.valueOf(com.natural.data.analyze.spark.user.visit.util.StringUtils.getFieldFromConcatString(
                sessionStr, "\\|", Constants.SESSION_COUNT));

        System.out.println("session count : " + session_count);








    }
}
