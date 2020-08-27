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

        collector.collect(new BrandLike());
    }
}
