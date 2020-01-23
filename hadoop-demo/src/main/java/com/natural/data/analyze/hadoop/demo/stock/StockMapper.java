package com.natural.data.analyze.hadoop.demo.stock;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    /*


    日期	股票代码	名称	收盘价	最高价	最低价	开盘价	前收盘	涨跌额	涨跌幅	成交量
    2020/1/21	'600519	贵州茅台	1075.3	1087	1072.3	1081	1091	-15.7	-1.439	3287405


  */

    Text outputKey = new Text();
    DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length == 11) {
            // 提取日期   2020
            String date = fields[0].substring(0, 4);
            Double price = null;
            try {
                price = Double.parseDouble(fields[3]);
            } catch (NumberFormatException e) {
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
            outputKey.set(date);
            outputValue.set(price.doubleValue());

            context.write(outputKey, outputValue);
        }


    }
}
