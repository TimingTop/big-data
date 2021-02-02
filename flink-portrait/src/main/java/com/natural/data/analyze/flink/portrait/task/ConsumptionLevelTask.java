package com.natural.data.analyze.flink.portrait.task;

import com.natural.data.analyze.flink.portrait.entity.ConsumptionLevel;
import com.natural.data.analyze.flink.portrait.map.ConsumptionLevelMap;
import com.natural.data.analyze.flink.portrait.reduce.ConsumptionLevelFinalReduce;
import com.natural.data.analyze.flink.portrait.reduce.ConsumptionLevelReduce;
import jdk.jshell.spi.ExecutionEnv;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;

import java.util.List;

/***
 * 使用订单表的数据 计算 消费级别
 *
 *
 *
 *
 *
 *
 */
public class ConsumptionLevelTask {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> text = env.readTextFile("data/order.txt");
        MapOperator<String, ConsumptionLevel> mapResult = text.map(new ConsumptionLevelMap());

        GroupReduceOperator<ConsumptionLevel, ConsumptionLevel> reduceResult =
                mapResult.groupBy("groupField").reduceGroup(new ConsumptionLevelReduce());

        ReduceOperator<ConsumptionLevel> reduceResultFinal =
                reduceResult.groupBy("groupField").reduce(new ConsumptionLevelFinalReduce());


        try {
            List<ConsumptionLevel> resultList = reduceResultFinal.collect();

            for (ConsumptionLevel item : resultList) {
                String type = item.getConsumptionType();
                Long count = item.getCount();

                System.out.println(type + ":" + count);
            }

//            env.execute("ConsumptionLevelTask !!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
