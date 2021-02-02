package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.ConsumptionLevel;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ConsumptionLevelFinalReduce implements ReduceFunction<ConsumptionLevel> {
    @Override
    public ConsumptionLevel reduce(ConsumptionLevel consumptionLevel, ConsumptionLevel t1) throws Exception {
        String type = consumptionLevel.getConsumptionType();
        Long count1 = consumptionLevel.getCount();
        Long count2 = t1.getCount();

        ConsumptionLevel result = new ConsumptionLevel();
        consumptionLevel.setConsumptionType(type);
        consumptionLevel.setCount(count1 + count2);
        return consumptionLevel;
    }
}
