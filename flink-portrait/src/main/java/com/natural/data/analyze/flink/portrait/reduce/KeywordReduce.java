package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.KeywordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

public class KeywordReduce implements ReduceFunction<KeywordEntity> {

    @Override
    public KeywordEntity reduce(KeywordEntity keywordEntity, KeywordEntity t1) throws Exception {
        String userId = keywordEntity.getUserId();

        List<String> words1 = keywordEntity.getOriginalWords();
        List<String> words2 = t1.getOriginalWords();

        List<String> finalWords = new ArrayList<>();
        finalWords.addAll(words1);
        finalWords.addAll(words2);

        KeywordEntity result = new KeywordEntity();
        result.setOriginalWords(finalWords);
        result.setUserId(userId);

        return result;

    }
}
