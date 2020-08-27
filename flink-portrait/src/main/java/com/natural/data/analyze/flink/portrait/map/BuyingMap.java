package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.BuyingInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class BuyingMap implements MapFunction<String, BuyingInfo> {
    @Override
    public BuyingInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }

        String[] orderInfos = s.split(",");
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

        BuyingInfo buyingInfo = new BuyingInfo();
        buyingInfo.setUserId(userId);
        buyingInfo.setCreateTime(createTime);
        buyingInfo.setAmount(amount);
        buyingInfo.setPayType(payType);
        buyingInfo.setPayTime(payTime);
        buyingInfo.setPayStatus(payStatus);
        buyingInfo.setCouponAmount(couponAmount);
        buyingInfo.setTotalAmount(totalAmount);
        String groupField = "buyinginfo==" + userId;
        buyingInfo.setGroupField(groupField);
        List<BuyingInfo> list = new ArrayList<>();
        list.add(buyingInfo);
        buyingInfo.setList(list);

        return buyingInfo;

    }
}
