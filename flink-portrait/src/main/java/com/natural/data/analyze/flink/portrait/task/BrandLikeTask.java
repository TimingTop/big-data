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
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;


/***
 * * 因为要使用 stream ，所以 需要 watermark + window 技术
 *  *
 *  * watermark 是 定时产生一些 带时间的消息，window 拿到这些时间消息，判断一下
 *  * 有没有到达 自己的下限，然后 触发计算
 *  *
 *  * window 可以延时计算，因为有一些窗口内的消息 会 延时 才到达
 *  * 如果该 消息太迟了，只能丢弃计算，
 *  *
 *  * stream 可以做到实时性很好，但是可能会损失一些精度，因为有些消息迟到了，有可能
 *  * 被丢弃
 *
 *
 *
 *
 */
public class BrandLikeTask {

    public static void main(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableSysoutLogging();

        see.setParallelism(1);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // source: socket
        DataStreamSource<String> input3 = see.socketTextStream("localhost", 9999);

        DataStream<String> input = see.addSource(new SocketTextStreamFunction("localhost", 9999, ",", 5))
                .assignTimestampsAndWatermarks(new CustonWater());
        // source: kafka
//        see.addSource(new FlinkKafkaconsu)
        // input format:
        //
        SingleOutputStreamOperator<BrandLike> brandLikeMap = input.flatMap(new BrandLikeMap());


        DataStream<BrandLike> brandLikeReduce = brandLikeMap.keyBy("groupByField")
                .timeWindowAll(Time.seconds(2))
                .reduce(new BrandLikeReduce());

        // 保存到 db 中
        brandLikeReduce.addSink(new BrandLikeSink());

        try {
            see.execute("brankLike analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private static class CustonWater implements AssignerWithPeriodicWatermarks<String> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            return 0;
        }
    }







}
