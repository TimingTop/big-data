package com.natural.data.analyze.hadoop.demo.invertedIndex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// hadoop jar hadoop-demo-1.0.0.jar /tmp/input/demo2 /tmp/output/demo2

public class RevertedIndexApplication {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // todo 不知道为什么 yarn 为报错
//        configuration.set("mapreduce.framework.name", "local");

        configuration.set("yarn.application.classpath", "/opt/hadoop-3.1.2/share/hadoop/mapreduce/*,/opt/hadoop-3.1.2/share/hadoop/mapreduce/lib/*," +
                "/opt/hadoop-3.1.2/share/hadoop/common/*,/opt/hadoop-3.1.2/share/hadoop/common/lib/*,/opt/hadoop-3.1.2/share/hadoop/yarn/*," +
                "/opt/hadoop-3.1.2/share/hadoop/yarn/lib/*,/opt/hadoop-3.1.2/share/hadoop/hdfs/*,/opt/hadoop-3.1.2/share/hadoop/hdfs/lib/*");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(RevertedIndexApplication.class);

        job.setMapperClass(RevertedIndexMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setReducerClass(RevertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);
    }
}
