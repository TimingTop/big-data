package com.natural.data.analyze.flink.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketTextStream = env.socketTextStream("localhost", 9000);
        
        // map
        DataStream<String> socket_message = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return Joiner.on(":").join("socket message", s);
            }
        });


        socket_message.print();

        env.execute();
    }
}
