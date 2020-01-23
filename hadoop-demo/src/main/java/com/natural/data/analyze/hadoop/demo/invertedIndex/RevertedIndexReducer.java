package com.natural.data.analyze.hadoop.demo.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RevertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 存储 不同文件下同一个单词的次数
        Map<String, Integer> wordMap = new HashMap<>();

        for (Text fileName : values) {
            // filename:1
            String[] words = (fileName.toString()).split(":");
            // 拿到文件名
            String file = words[0];
            int count = Integer.valueOf(words[1]);
            // 这个类针对的就是同一个key，就是同一个单词，统计一下同一个单词， 在同一个文件下的次数
            if (wordMap.containsKey(file)) {
                count += wordMap.get(file);
            }
            wordMap.put(file, count);
        }

        // 遍历Hashmap, 输出字段
        StringBuilder sb = new StringBuilder();
        Iterator iterator = wordMap.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Object index = entry.getKey();
            Object value = entry.getValue();
            sb.append(index.toString() + ":" + value.toString() + ";");
        }

        context.write(key, new Text(sb.toString()));
    }
}
