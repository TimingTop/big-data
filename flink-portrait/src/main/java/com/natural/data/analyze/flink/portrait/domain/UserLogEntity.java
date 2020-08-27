package com.natural.data.analyze.flink.portrait.domain;


/**
 *
 * 用户操作商品的日志结构
 *
 *
 *
 *
 *
 */
public class UserLogEntity {
    private int userId;
    private int productId;
    private long time;   // 操作时间
    private String action; // 操作类型： 点击，购买， 浏览


    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
