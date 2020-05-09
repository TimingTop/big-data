package com.natural.data.analyze.spark.user.visit.session;

import com.natural.data.analyze.spark.user.visit.constant.Constants;
import com.natural.data.analyze.spark.user.visit.util.DateUtils;
import com.natural.data.analyze.spark.user.visit.util.SimulateData;
import com.natural.data.analyze.spark.user.visit.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;


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



        boolean isLocal = true;



        // 创建 sparksession

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("testone");
        if (isLocal) {
            builder.master("local[1]");
        }
        SparkSession spark = builder.getOrCreate();

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


        //  get use session
        Dataset<Row> userSessionResult = spark.sql("select * from user_visit_action");

//        JavaRDD<Row> actionRDD = userSessionResult.javaRDD().repartition(10);

//        userSessionResult.show(2);
        JavaRDD<Row> actionRDD = userSessionResult.javaRDD();

        // （session_id, Row）
        JavaPairRDD<String, Row> session2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        // 持久化数据到 内存
        session2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        // 对用户行为分组
        // (session_id, [Row1, Row2, Row3])
        JavaPairRDD<String, Iterable<Row>> session2actionRDDS = session2actionRDD.groupByKey();
//        System.out.println(session2actionRDDS.count());
        // session 表 跟 user 表聚合，查出用户id
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
                    if (row == null) {
                        continue;
                    }
                    if (useId == null) {
                        useId = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);
                    // 这个会抛错误，因为有可能为null
//                    String clickCategoryIdStr = row.getString(6);
                    Long clickCategoryId = null;

                    if (!row.isNullAt(6)) {
                        clickCategoryId = row.getLong(6);
                    }

                    if (searchKeyword != null && "".equals(searchKeyword)) {
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

                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (startTime != null && actionTime !=null && actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (endTime != null && actionTime != null && actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    stepLength++;

                }

                String searchKeywords = StringUtils
                        .trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils
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

//        System.out.println(userId2aggrInfoRDD.count());
        //    =================   查 用户
        JavaRDD<Row> userInfoRDD = spark.sql("select * from user_info").javaRDD();

//        Dataset<Row> sql = spark.sql("select * from user_info");
//        sql.show(2);
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        // ========================= end

        JavaPairRDD<Long, Tuple2<String, Row>> userId2fullInfoRDD = userId2aggrInfoRDD.join(userId2InfoRDD);
//        System.out.println(userId2aggrInfoRDD.count());
        // (sessionId, fullInfoRow)
        JavaPairRDD<String, String> sessionId2fullAggrInfoRDD = userId2fullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String aggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionId = StringUtils
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

        long count = sessionId2fullAggrInfoRDD.count();
        System.out.println("count: " + count);

        sessionId2fullAggrInfoRDD.take(10).forEach(
                (x) -> {
                    System.out.println(x._1 + ":" + x._2);
                }
        );
        // 自定义统计
        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();

        spark.sparkContext().register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");




        // 过滤一部分的数据，然后还要统计一下，需要用到的 session 数据
        JavaPairRDD<String, String> filterSessionId2aggrInfoRDD = sessionId2fullAggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;

                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
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

//

        filterSessionId2aggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

//        filterSessionId2aggrInfoRDD.take(10).forEach(
//                (x) -> {
//                    System.out.println(x._1 + ":" + x._2);
//                }
//        );
//        long count1 = filterSessionId2aggrInfoRDD.count();
//        System.out.println(count1);

        // 输出 每个 统计的数据
//        String sessionStr = sessionAggrStatAccumulator.value();
//        System.out.println(sessionStr);
//        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
//                sessionStr, "\\|", Constants.SESSION_COUNT));
//
//        System.out.println("session count : " + session_count);

        // ########################## 计算每天每小时的session 数 ##################################
        // <sessionId, [x|y|z]>    ==>   <yyyy-MM-dd_HH, sessionId>
        //  <839efd8408214987b128507ec7603717 ,
        //  sessionid=839efd8408214987b128507ec7603717|searchKeywords=|clickCategoryIds=43|visitLength=66996|stepLength=10|startTime=2020-05-07 04:02:07|age=28|professional=professional64|city=city89|sex=female>
        JavaPairRDD<String, String> time2SessionRDD = sessionId2fullAggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                String aggrInfo = tuple2._2;
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo,
                        "\\|", Constants.FIELD_START_TIME);

                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<>(dateHour, aggrInfo);
            }
        });

        // 每天每小时 session 数
        Map<String, Long> countMap = time2SessionRDD.countByKey();

        countMap.forEach(
                (x, y) -> {
                    System.out.println(x + ":" + y);
                }
        );


        // 选择 top N 点击商品


//        SortTop10Category.getTop10Category(filterSessionId2aggrInfoRDD, session2actionRDD);





    }
}
