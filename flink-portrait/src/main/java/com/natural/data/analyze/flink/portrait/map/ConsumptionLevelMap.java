package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.consume.ConsumeIndexTask;
import com.natural.data.analyze.flink.portrait.entity.ConsumptionLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class ConsumptionLevelMap implements MapFunction<String, ConsumptionLevel> {

    @Override
    public ConsumptionLevel map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        String [] orderInfos = s.split(",");
        String id = orderInfos[0];
        String productId = orderInfos[1];
        String productTypeId = orderInfos[2];
        String createTime = orderInfos[3];
        String amount = orderInfos[4];
        String payType = orderInfos[5];
        String payTime = orderInfos[6];
        String payStatus = orderInfos[7];
        String couponAmount = orderInfos[8];
        String totalAmount = orderInfos[9];
        String refundAmount = orderInfos[10];
        String num = orderInfos[11];
        String userId = orderInfos[12];

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setUserId(userId);
        consumptionLevel.setAmountTotal(totalAmount);
        consumptionLevel.setGroupField("==consumptionLevel==" + userId);

        return consumptionLevel;
    }
}
