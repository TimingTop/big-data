package com.natural.data.analyze.core.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerStreamDemo1 {

    public static void main(String[] args) {
        Properties p = new Properties();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();
//        builder.<String, String>stream("aaa")
//                .mapValues(value -> String.valueOf(value.length()))
//                .to("aaa-two");
//        KafkaStreams streams = new KafkaStreams(builder.build(), p);
        //streams.start();

        KStream<String, String> textLines = builder.stream("stream-one", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\w+")))
                .groupBy((key, value) -> value)
                .count();

        wordCounts.toStream()
                .to("stream-two", Produced.with(Serdes.String(), Serdes.Long()));

    }
}
