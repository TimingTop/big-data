package com.natural.data.analyze.flink.demo.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple2;

import java.io.IOException;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2128");
        properties.setProperty("group.id", "test1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new FlinkKafkaConsumer<>("aaa", new SimpleStringSchema(), properties));

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.setStartFromLatest();
        consumer.setStartFromGroupOffsets();


        ValueState<Tuple2<Long, Long>> sum = null;

        Tuple2<Long, Long> currentSum = sum.value();






    }
}
