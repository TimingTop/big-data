package com.natural.data.analyze.hadoop.demo.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StockApplication {

    /*

    计算 股票 在一段时间内，每一年内的最高收盘价

     */


    /*

    2016    337.76
    2017    719.11
    2018    799.19
    2019    1233.75
    2020    1130.0

    */

    // hadoop jar hadoop-demo-1.0.0.jar /tmp/input/demo3 /tmp/output/demo3
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(StockApplication.class);

        job.setMapperClass(StockMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(StockCombiner.class);

        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
