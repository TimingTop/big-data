package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.BrandLike;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Calmo
 */
public class BrandLikeSink implements SinkFunction<BrandLike> {
    @Override
    public void invoke(BrandLike value, Context context) throws Exception {
        String brand = value.getBrand();
        long count = value.getCount();

        // 存进  mongo db 中
        System.out.println("info=" + brand);
        System.out.println("count=" + count);

    }
}
