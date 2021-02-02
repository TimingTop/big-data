package com.natural.data.analyze.flink.portrait.task;


import com.natural.data.analyze.flink.portrait.entity.BuyingInfo;
import com.natural.data.analyze.flink.portrait.map.BuyingMap;
import com.natural.data.analyze.flink.portrait.reduce.BuyingReduce;
import com.natural.data.analyze.flink.portrait.util.DateUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 任务：  购物指数
 *
 * source： 数据来源
 *       1. order info   订单表
 *            id               订单id
 *            productId         产品id
 *            productTypeId     产品类型id
 *            createtime        订单创建时间       yyyyMMdd hhmmss
 *            amount             ? 商品数量
 *            paytype           支付类型
 *            paytime           支付时间
 *            paystatus         支付状态: 0=未支付，1=已支付， 2=已退款
 *            couponamount      优惠券数量
 *            totalamount       总金额    这张订单最大的金额
 *            refundamout       退款
 *            num               ?
 *            userid            用户id
 *
 *
 *
 *
 */
public class BuyingIndexTask {

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        /**
         * 数据源：  1. socket  2. kafka 3. 文件log
         */
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().setGlobalJobParameters(params);
        // 数据源3 文件log
        DataStreamSource<String> text = see.readTextFile("");
        DataSource<String> text2 = env.readTextFile("");

        // 提取数据
        DataSet<BuyingInfo> mapResult = text2.map(new BuyingMap());
        // 按照用户 分组后，再做 聚合
        ReduceOperator<BuyingInfo> reduceResult = mapResult.groupBy("groupField")
                .reduce(new BuyingReduce());

        // <userId, [BuyingInfo]>

        try {
            // 收集结果
            List<BuyingInfo> resultList = reduceResult.collect();
            for (BuyingInfo info : resultList) {
                String userId = info.getUserId();
                List<BuyingInfo> list = info.getList();

                // 把消费时间 排序  倒序排
                Collections.sort(list, new Comparator<BuyingInfo>() {
                    @Override
                    public int compare(BuyingInfo o1, BuyingInfo o2) {
                        String timeO1 = o1.getCreateTime();
                        String timeO2 = o2.getCreateTime();

                        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
                        Date dateNow = new Date();
                        Date time1 = dateNow;
                        Date time2 = dateNow;

                        try {
                            time1 = dateFormat.parse(timeO1);
                            time2 = dateFormat.parse(timeO2);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        return time1.compareTo(time2);
                    }
                });

                // 计算

                BuyingInfo before = null;

                Map<Integer, Integer> frequencyMap = new HashMap<>();
                double maxAmount = 0d;
                double sum = 0d;

                for (BuyingInfo item : list) {

                    if (before == null) {
                        before = item;
                        continue;
                    }

                    // 计算购买频率
                    String beforeTime = before.getCreateTime();
                    String endTime = item.getCreateTime();
                    int days = DateUtils.getDaysBetweenByStartAndEnd(beforeTime, endTime, "yyyyMMdd hhmmss");

                    int frequencyDay = frequencyMap.get(days) == null ? 0 : frequencyMap.get(days);
                    frequencyMap.put(days, frequencyDay + 1);

                    // 计算 最大金额, 在所有订单中 找到 最大最大的那张
                    String totalAmountString = item.getTotalAmount();
                    Double totalAmount = Double.valueOf(totalAmountString);

                    if (totalAmount > maxAmount) {
                        maxAmount = totalAmount;
                    }

                    // 计算平均值, 先把 总消费 记录下来，退出 循环后再计算 平均值
                    sum += totalAmount;

                    before = item;
                }
                // 计算平均值
                double avrAmount = sum / list.size();

                int totalDays = 0;

                for (Map.Entry<Integer, Integer> entry : frequencyMap.entrySet()) {
                    Integer frequencyDays = entry.getKey();
                    Integer count = entry.getValue();
                    totalDays += frequencyDays * count;
                }

                // 平均天数，就是 平均多少天 会购物一次
                int avrDays = totalDays / list.size();

                // 购物指数
                // 支付金额平均值 * 0.3
                // 最大支付金额 * 0.3
                // 下单频率 * 0.4


                int avrAmountScore = 0;
                if (avrAmount >= 0 && avrAmount < 20) {
                    avrAmountScore = 5;
                } else if (avrAmount >= 20 && avrAmount < 20) {
                    avrAmountScore = 10;
                }


            }




        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
