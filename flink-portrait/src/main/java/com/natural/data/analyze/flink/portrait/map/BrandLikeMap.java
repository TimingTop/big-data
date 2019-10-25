package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.BrandLike;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author Calmo 2019-10-14
 */
public class BrandLikeMap implements FlatMapFunction<String, BrandLike> {

    //  userId,brand,createTime,
    @Override
    public void flatMap(String s, Collector<BrandLike> collector) throws Exception {
        String[] split = s.split(",");

        collector.collect(new BrandLike());
    }
}
