package com.natural.data.analyze.flink.portrait.entity;

import java.util.List;

public class BuyingInfo {
    private String buyingIndexType; // 购物指数区段： 0-20 20-50 50-70 70-80 80-90 90-100
    private String userId;
    private String createTime;
    private String amount;
    private String payType;
    private String payTime;
    private String payStatus; // 0=未支付  1=已支付  2=已退款
    private String couponAmount; // 优惠券
    private String totalAmount;  // 总金额,  这张 订单 最大的金额
    private String refundAmount; // 退款
    private Long count;
    private String groupField; // 分组信息，用来分组的标志符

    private List<BuyingInfo> list; // 汇总的时候需要用到



    //  ************* getter & setter ************************
    public String getBuyingIndexType() {
        return buyingIndexType;
    }

    public void setBuyingIndexType(String buyingIndexType) {
        this.buyingIndexType = buyingIndexType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getPayTime() {
        return payTime;
    }

    public void setPayTime(String payTime) {
        this.payTime = payTime;
    }

    public String getPayStatus() {
        return payStatus;
    }

    public void setPayStatus(String payStatus) {
        this.payStatus = payStatus;
    }

    public String getCouponAmount() {
        return couponAmount;
    }

    public void setCouponAmount(String couponAmount) {
        this.couponAmount = couponAmount;
    }

    public String getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(String totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(String refundAmount) {
        this.refundAmount = refundAmount;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public List<BuyingInfo> getList() {
        return list;
    }

    public void setList(List<BuyingInfo> list) {
        this.list = list;
    }
}
