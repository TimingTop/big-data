package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.KeywordEntity;
import com.natural.data.analyze.flink.portrait.util.IKUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class KeywordMap2 implements MapFunction<KeywordEntity, KeywordEntity> {

    @Override
    public KeywordEntity map(KeywordEntity keywordEntity) throws Exception {
        List<String> words = keywordEntity.getOriginalWords();
        // 保存每个词出现的次数
        Map<String, Long> tfMap = new HashMap<>();
        // 保存 出现过的词
        Set<String> wordSet = new HashSet<>();

        for (String item : words) {
            // 使用分词工具 把 词分掉
            List<String> listData = IKUtil.getIKWord(item);
            // 把分解出来的词保存起来， 并且把 出现的次数计算出来
            for (String word : listData) {
                Long value = tfMap.get(word) == null? 0L : tfMap.get(word);
                tfMap.put(word, value + 1);
                wordSet.add(word);
            }
        }

        KeywordEntity result = new KeywordEntity();
        String userId = keywordEntity.getUserId();
        result.setUserId(userId);
        result.setDataMap(tfMap);

        /**
         *
         * 计算 每个词 在 总词量 出现的 占比率，就知道 那个是 高频词
         *
         *
         */
        // 1。 计算 总词量
        long sum = 0L;
        Collection<Long> values = tfMap.values();
        for (Long temp : values) {
            sum += temp;
        }
        // 2. 计算 每个词的占比率
        Map<String, Double> tfMapFinal = new HashMap<>();
        Set<Map.Entry<String, Long>> entries = tfMap.entrySet();

        for (Map.Entry<String, Long> temp : entries) {
            String word = temp.getKey();
            long count = temp.getValue();
            double tf = Double.valueOf(count) / Double.valueOf(sum);

            tfMapFinal.put(word, tf);
        }
        result.setTfMap(tfMapFinal);


        return result;
    }
}
