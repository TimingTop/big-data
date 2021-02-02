package com.natural.data.analyze.flink.portrait.task;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 计算用户的营运商， 移动，联通，电信 的用户量
 *
 *
 *  这个比较简单
 *  把 所有用户的手机号码 判断一下，然后再根据上面的
 *
 *
 *
 *
 */
public class CarrierTask {

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSource<String> text = env.readTextFile("");

    }
}
