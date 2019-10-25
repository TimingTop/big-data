package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.BrandLike;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author Calmo    2019-10-14
 *
 */
public class BrandLikeReduce implements ReduceFunction<BrandLike> {
    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike t1) throws Exception {
        String brand = brandLike.getBrand();

        long count1 = brandLike.getCount();
        long count2 = t1.getCount();

        BrandLike result = new BrandLike();
        result.setBrand(brand);
        result.setCount(count1 + count2);
        return result;
    }
}
