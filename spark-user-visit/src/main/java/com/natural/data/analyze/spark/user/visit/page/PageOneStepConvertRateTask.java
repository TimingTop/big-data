package com.natural.data.analyze.spark.user.visit.page;

import com.natural.data.analyze.spark.user.visit.util.DateUtils;
import com.natural.data.analyze.spark.user.visit.util.SimulateData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

public class PageOneStepConvertRateTask {
  public static void main(String[] args) {
     // 创建上下文
      boolean isLocal = true;
      SparkSession.Builder builder = SparkSession.builder();
      builder.appName("testone");
      if (isLocal) {
          builder.master("local[1]");
      }
      SparkSession spark = builder.getOrCreate();

      SimulateData.simulation(spark.sqlContext());
      // 获取指定范围内的用户行为数据
      Dataset<Row> userSessionResult = spark.sql("select * from user_visit_action");
      JavaRDD<Row> actionRDD = userSessionResult.javaRDD();

      // ####  开始转换
      JavaPairRDD<String, Row> sessionId2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
          @Override
          public Tuple2<String, Row> call(Row row) throws Exception {
              String sessionId = row.getString(2);
              return new Tuple2<>(sessionId, row);
          }
      });

      sessionId2actionRDD = sessionId2actionRDD.cache();
        // 分组统计
      JavaPairRDD<String, Iterable<Row>> sessionId2actionsRDD = sessionId2actionRDD.groupByKey();


      // 计算的是页面单跳转化率，https://blog.csdn.net/wuxintdrh/article/details/81052858
      // 指定   3,5,7
      // 3->5,  5->7  的访问量， 两两相除，得出转化率

      // 准备计算单跳页面
      // <sessionId, [row, row, row]>
      //  ==> <pageId2_pageId2, count>     <1_2, 1>
      // https://www.jianshu.com/p/f478376bdbb9
//      spark.sparkContext().broadcast()
      String pageFlow = "234,345,3456,5467";
      ClassTag tag = ClassTag$.MODULE$.apply(String.class);
      Broadcast<String> broadcast = spark.sparkContext().broadcast(pageFlow, tag);

      // <1_2, 1>
      // <2_3, 1>
      // <3_4, 1>
      JavaPairRDD<String, Integer> pageSplitRDD = sessionId2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
          @Override
          public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
              List<Tuple2<String, Integer>> list = new ArrayList<>();

              Iterator<Row> iterator = tuple2._2.iterator();
              List<Row> rows = new ArrayList<>();
              while (iterator.hasNext()) {
                  rows.add(iterator.next());
              }
              // 时间倒序
              Collections.sort(rows, new Comparator<Row>() {
                  @Override
                  public int compare(Row o1, Row o2) {
                      String actionTime1 = o1.getString(4);
                      String actionTime2 = o2.getString(4);
                      Date date1 = DateUtils.parseTime(actionTime1);
                      Date date2 = DateUtils.parseTime(actionTime2);
                      return (int) (date1.getTime() - date2.getTime());
                  }
              });

              String[] targetPages = broadcast.value().split(",");
              Long lastPageId = null;
              // 因为是 时间倒序，所以最新的在最前
              // 这个要测试一下，要时间 升序 才有效果
              for (Row row : rows) {
                  long pageId = row.getLong(3);
                  if (lastPageId == null) {
                      lastPageId = pageId;
                      continue;
                  }
                  String pageSplit = lastPageId + "_" + pageId;
                  for (int i = 1; i < targetPages.length; i++) {
                      String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                      if (pageSplit.equals(targetPageSplit)) {
                          list.add(new Tuple2<>(pageSplit, 1));
                          break;
                      }
                  }
                  //
                  lastPageId = pageId;
              }
              return list.iterator();
          }
      });
      // <1_2, 1> <2_3, 1>   ==> <1_2, 24>, <2_3, 3>
      Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();

      // 计算第一个页面的 pv



      sessionId2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {

          @Override
          public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
              return null;
          }
      });




  }
}
