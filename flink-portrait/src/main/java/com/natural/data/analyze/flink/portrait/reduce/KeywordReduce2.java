package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.KeywordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class KeywordReduce2 implements ReduceFunction<KeywordEntity> {

    @Override
    public KeywordEntity reduce(KeywordEntity keywordEntity, KeywordEntity t1) throws Exception {
        long count1 = keywordEntity.getTotalDocument() == null ? 1L : keywordEntity.getTotalDocument();
        long count2 = t1.getTotalDocument() == null ? 1L : t1.getTotalDocument();

        KeywordEntity result = new KeywordEntity();
        result.setTotalDocument(count1 + count2);
        return result;
    }
}
