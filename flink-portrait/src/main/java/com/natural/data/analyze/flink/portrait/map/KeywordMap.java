package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.KeywordEntity;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class KeywordMap implements MapFunction<String, KeywordEntity> {

    @Override
    public KeywordEntity map(String s) throws Exception {
        String[] productWordArray = s.split(",");
        String userId = productWordArray[0];
        String wordArray = productWordArray[1];

        KeywordEntity keywordEntity = new KeywordEntity();
        keywordEntity.setUserId(userId);
        List<String> words = new ArrayList<>();
        words.add(wordArray);
        keywordEntity.setOriginalWords(words);
        return keywordEntity;
    }
}
