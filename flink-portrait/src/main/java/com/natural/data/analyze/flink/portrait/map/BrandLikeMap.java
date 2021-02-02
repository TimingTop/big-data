package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.BrandLike;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author Calmo 2019-10-14
 *
 * 品牌喜爱
 * 

 *
 *
 *
 *
 *
 *
 */
public class BrandLikeMap implements FlatMapFunction<String, BrandLike> {

    /**
     * productid: 产品id
     * producttypeid: 产品类型id
     * scantime: 浏览时间
     * staytime: 停留时间
     * userid: 用户id
     * usertype: 用户浏览终端，0=pc, 1=app, 2=小程序
     * ip: ip 地址
     * brand: 品牌
     *
     *
     *
     */
    @Override
    public void flatMap(String s, Collector<BrandLike> collector) throws Exception {
        String[] split = s.split(",");
        // 从 存储系统获取 之前保存的 品牌 购买次数
        // 因为 stream 是不保存 之前的状态的，需要中间状态的
        String maxBrand = "xiaomi";
        long maxSize = 1L;

        BrandLike brandLike = new BrandLike();
        brandLike.setBrand(maxBrand);
        brandLike.setCount(maxSize);

        brandLike.setGroupByField("==brandLike==" + maxBrand);

        collector.collect(brandLike);
    }
}
