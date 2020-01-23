package com.natural.data.analyze.hadoop.demo.stock;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class StockReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    DoubleWritable outputValue = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> iterator = values.iterator();
        double maxValue = 0.0;

        while (iterator.hasNext()) {
            DoubleWritable value = iterator.next();
            if (value.get() > maxValue) {
                maxValue = value.get();
            }
        }
        outputValue.set(maxValue);
        context.write(key, outputValue);
    }
}
