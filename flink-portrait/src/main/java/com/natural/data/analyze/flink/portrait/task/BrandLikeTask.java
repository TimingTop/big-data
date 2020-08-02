package com.natural.data.analyze.flink.portrait.task;

import com.natural.data.analyze.flink.portrait.entity.BrandLike;
import com.natural.data.analyze.flink.portrait.map.BrandLikeMap;
import com.natural.data.analyze.flink.portrait.reduce.BrandLikeReduce;
import com.natural.data.analyze.flink.portrait.reduce.BrandLikeSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BrandLikeTask {

    public static void main(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        see.setParallelism(1);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> input = see.socketTextStream("localhost", 9999);

        // input format:
        //
        SingleOutputStreamOperator<BrandLike> brandLikeMap = input.flatMap(new BrandLikeMap());

        DataStream<BrandLike> brandLikeReduce = brandLikeMap.keyBy("groupByField")
                .timeWindowAll(Time.seconds(2))
                .reduce(new BrandLikeReduce());

        // 保存到 db 中
        brandLikeReduce.addSink(new BrandLikeSink());


    }
}
