package com.natural.data.analyze.flink.demo.student;

import com.google.gson.Gson;
import com.natural.data.analyze.flink.demo.student.model.Answer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Test {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "xxx:9092,xxx:9092");
        prop.setProperty("group.id", "aa");

        FlinkKafkaConsumer<String> flinkKafkaSource = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), prop);
        DataStreamSource<String> dataStream = env.addSource(flinkKafkaSource);


        SingleOutputStreamOperator<Answer> map = dataStream.map(new MapFunction<String, Answer>() {
            @Override
            public Answer map(String s) throws Exception {
                Gson gson = new Gson();

                Answer answer = gson.fromJson(s, Answer.class);
                return answer;
            }
        });

//        StreamTableEnviron

    }
}
