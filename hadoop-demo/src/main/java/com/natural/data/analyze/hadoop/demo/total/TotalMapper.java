package com.natural.data.analyze.hadoop.demo.total;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TotalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    Text outputKey = new Text();
    DoubleWritable outputValue = new DoubleWritable();
    // 0 1 2 3 1999-10 50
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split("\\s+");

        System.out.println(columns.length);

        String year = columns[4].substring(0, 4);
        Double sendMoney = Double.valueOf(columns[5]);

        outputKey.set(year);
        outputValue.set(sendMoney);

        context.write(outputKey, outputValue);
    }

    public static void main(String[] args) {
        String line = "0 1 2 3 2000-10 95.8";
        String[] columns = line.split("\\s+");
        String year = columns[4].substring(0, 4);
        Double sendMoney = Double.valueOf(columns[5]);

        System.out.println(year);
        System.out.println(sendMoney);
    }
}
