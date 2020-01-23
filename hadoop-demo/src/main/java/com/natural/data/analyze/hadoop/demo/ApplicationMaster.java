package com.natural.data.analyze.hadoop.demo;


import com.natural.data.analyze.hadoop.demo.total.TotalMapper;
import com.natural.data.analyze.hadoop.demo.total.TotalReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ApplicationMaster {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // total
        // 1. 获取配置信息
        Configuration configuration = new Configuration();
        // 配置hadoop的集群地址
//        configuration.set("fs.defaultFS", "hdfs://192.168.56.106:8020");
        // 配置本地模式, mapred-default.xml
        // 参考 https://hadoop.apache.org/docs/r3.1.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml
        configuration.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(configuration);
        // 2. 加载jar的启动路径
        job.setJarByClass(ApplicationMaster.class);
        // 3. 设置map和reduce类
        job.setMapperClass(TotalMapper.class);
        job.setReducerClass(TotalReducer.class);

//        job.setCombinerClass();
        // 4. 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // 5. 设置最终kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // 6. 设置输入和输出路径
        // 设置为读取 args 的参数输入
//        FileInputFormat.addInputPath(job, new Path("C:\\personal\\big-data-root\\hadoop-demo\\src\\main\\java\\com\\natural\\data\\analyze\\hadoop\\demo\\total\\data1.csv"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\personal\\big-data-root\\hadoop-demo\\src\\main\\java\\com\\natural\\data\\analyze\\hadoop\\demo\\total\\aa.txt"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
