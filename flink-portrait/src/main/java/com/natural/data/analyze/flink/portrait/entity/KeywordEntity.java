package com.natural.data.analyze.flink.portrait.entity;

import java.util.List;
import java.util.Map;

public class KeywordEntity {
    private String userId;
    private Map<String, Long> dataMap; // 存放每个词 出现的 次数
    private Map<String, Double> tfMap; // 存放每个词 出现的概率
    private Long totalDocument;
    // 经过 分词之后得到的词组
    private List<String> finalKeyword;
    // 保存 从用户评价那里 抠出来的句子 短语
    private List<String> originalWords;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Map<String, Long> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Long> dataMap) {
        this.dataMap = dataMap;
    }

    public Map<String, Double> getTfMap() {
        return tfMap;
    }

    public void setTfMap(Map<String, Double> tfMap) {
        this.tfMap = tfMap;
    }

    public Long getTotalDocument() {
        return totalDocument;
    }

    public void setTotalDocument(Long totalDocument) {
        this.totalDocument = totalDocument;
    }

    public List<String> getFinalKeyword() {
        return finalKeyword;
    }

    public void setFinalKeyword(List<String> finalKeyword) {
        this.finalKeyword = finalKeyword;
    }

    public List<String> getOriginalWords() {
        return originalWords;
    }

    public void setOriginalWords(List<String> originalWords) {
        this.originalWords = originalWords;
    }
}
