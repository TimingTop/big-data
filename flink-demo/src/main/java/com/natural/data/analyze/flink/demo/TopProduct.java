package com.natural.data.analyze.flink.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class TopProduct {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9990);

        socketTextStream.setParallelism(1);


        socketTextStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return null;
            }
        });

        socketTextStream.keyBy(0);

//        socketTextStream.




    }
}
