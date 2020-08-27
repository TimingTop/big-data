package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.BuyingInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

public class BuyingReduce implements ReduceFunction<BuyingInfo> {

    @Override
    public BuyingInfo reduce(BuyingInfo buyingInfo, BuyingInfo t1) throws Exception {
        String userId = buyingInfo.getUserId();

        List<BuyingInfo> list = buyingInfo.getList();
        List<BuyingInfo> list1 = t1.getList();

        List<BuyingInfo> finalList = new ArrayList<>();
        finalList.addAll(list);
        finalList.addAll(list1);

        BuyingInfo info = new BuyingInfo();
        info.setUserId(userId);
        info.setList(finalList);

        return info;
    }
}
