package com.natural.data.analyze.flink.demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context context, Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp current = state.value();

        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        current.count++;
        current.lastModified = context.timestamp();

        state.update(current);

        context.timerService().registerEventTimeTimer(current.lastModified + 30000);


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp result = state.value();

    }
}
