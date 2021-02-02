package com.natural.data.analyze.flink.portrait.task;

import com.natural.data.analyze.flink.portrait.entity.KeywordEntity;
import com.natural.data.analyze.flink.portrait.map.KeywordMap;
import com.natural.data.analyze.flink.portrait.map.KeywordMap2;
import com.natural.data.analyze.flink.portrait.reduce.KeywordReduce;
import com.natural.data.analyze.flink.portrait.reduce.KeywordReduce2;
import jdk.jshell.spi.ExecutionEnv;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;


/**
 *
 * 数据源是从 用户评论那里 抠出来
 *
 * 根式：
 *  userId,好看 小米 全面屏手机 金色 全网通
 *
 *  这个 只做 分词 统计， 关键字 分析 需要用 机器学习，
 *
 */
public class MonthKeywordTask {
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataSource<String> text = env.readTextFile("data/key");

        MapOperator<String, KeywordEntity> mapResult = text.map(new KeywordMap());
        // 把每个userId 聚合起来
        ReduceOperator<KeywordEntity> reduceResult = mapResult.groupBy("userId").reduce(new KeywordReduce());
        // 分析每一个 userId 所有的 评论
        MapOperator<KeywordEntity, KeywordEntity> mapResult2 = reduceResult.map(new KeywordMap2());
        ReduceOperator<KeywordEntity> reduceResult2 = mapResult2.reduce(new KeywordReduce2());




    }
}
