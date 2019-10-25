package com.natural.data.analyze.core.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PipeStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.104:9092,192.168.56.105:9092,192.168.56.106:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("streams-one").to("streams-tow");

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("stream-hook") {
            @Override
            public void run() {
                streams.close();
                countDownLatch.countDown();
            }
        });

        try {
            streams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}
