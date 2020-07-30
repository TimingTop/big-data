package com.natural.data.analyze.flink.demo.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import scala.Tuple3;

public class TestC {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(9000);

        DataStreamSource<String> text = env.socketTextStream("localhost", 9900);

        SingleOutputStreamOperator<String> filter = text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(s)) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        filter.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String s) throws Exception {
                String[] tokens = s.toLowerCase().split("\\w+");
                long eventTime = Long.parseLong(tokens[1]);

                return new Tuple3<>(tokens[0], eventTime, 1);
            }
        });



    }
}
