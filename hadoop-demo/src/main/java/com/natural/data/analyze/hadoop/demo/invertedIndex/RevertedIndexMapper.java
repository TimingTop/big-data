package com.natural.data.analyze.hadoop.demo.invertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**j
 *
 *
 *
 * 倒排索引
 *
 */
public class RevertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取数据的文件名
        String path = ((FileSplit)context.getInputSplit()).getPath().toString();
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        System.out.println("fileName: " + fileName);
        String line = value.toString();
        // 以空格的分词，一般不会用这种方法分词
        String[] words = line.split("\\s+");

        for(String word : words) {
            context.write(new Text(word), new Text(fileName + ":1"));
        }
    }
}
