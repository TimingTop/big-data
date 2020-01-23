package com.natural.data.analyze.hadoop.demo.total;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sumMoney = 0.0D;

        for (DoubleWritable money : values) {
            sumMoney += money.get();
        }

        outputValue.set(sumMoney);

        context.write(key, outputValue);
    }
}
