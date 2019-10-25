package com.natural.data.analyze.flink.demo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import scala.Tuple2;

public class App {

    /**
     * v 1.9.0
     *
     * @param args
     */
    public static void main(String[] args) {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                        return wikipediaEditEvent.getUser();
                    }
                });

        DataStream<Tuple2<String, Long>> aggregate = keyedEdits
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
//                        accumulator._1 = value.getUser();
                        return null;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                        return null;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                        return null;
                    }
                });

//        keyedEdits.timeWindow(Time.seconds(5))



        aggregate.print();
//        see.execute();

    }
}
