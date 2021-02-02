package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.ConsumptionLevel;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ConsumptionLevelReduce implements GroupReduceFunction<ConsumptionLevel, ConsumptionLevel> {

    @Override
    public void reduce(Iterable<ConsumptionLevel> iterable, Collector<ConsumptionLevel> collector) throws Exception {
        Iterator<ConsumptionLevel> iterator = iterable.iterator();
        int sum = 0;
        double totalAmount = 0.0;
        String userId = "-1";

        while (iterator.hasNext()) {
            ConsumptionLevel consumptionLevel = iterator.next();
            userId = consumptionLevel.getUserId();
            String amountTotal = consumptionLevel.getAmountTotal();
            double amountDouble = Double.valueOf(amountTotal);

            totalAmount += amountDouble;
            sum++;
        }

        // 平均消费，5000 高消费  ， 1000 中消费，  <1000 低消费
        double avrAmount = totalAmount / sum;
        String flag = "low";
        if (avrAmount >= 1000 && avrAmount < 5000) {
            flag = "middle";
        } else if (avrAmount >= 5000) {
            flag = "high";
        }

        // 从db 那里 获取 改用户的，查到当前 消费 变化了，就产生一条消息 去抵消掉 上一个 产生的错误消息
        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setConsumptionType(flag);
        consumptionLevel.setCount(1L);
        // 不需要用户了，下一步是 计算 整个系统有多少个 高消费，低消费的之类的
        // 关于用户就是这个  reduce 计算的，计算完了还要 入库，
//        consumptionLevel.setUserId();
        consumptionLevel.setGroupField("==consumptionLevelFinal==" + flag);

        collector.collect(consumptionLevel);
    }
}
