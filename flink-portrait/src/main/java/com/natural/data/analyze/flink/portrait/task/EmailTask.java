package com.natural.data.analyze.flink.portrait.task;


import com.natural.data.analyze.flink.portrait.entity.EmailInfo;
import com.natural.data.analyze.flink.portrait.map.EmailMap;
import com.natural.data.analyze.flink.portrait.reduce.EmailReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

/***
 *
 *
 * 1. 数据来源：  用户表
 * 2.
 *
 * 数据源是静态数据，所以使用 dataset 进行操作
 *
 *
 *
 *      用户表    user
 *      0. userId
 *      1. username
 *      2. sex
 *      3. telephone
 *      4. email          邮件
 *      5. age      1,2,3
 *      6. registerTime    yyyyMMddHHmmss
 *      7. userType       0=pc  1=移动端  2=小程序
 *
 *
 *
 *
 *
 */
public class EmailTask {

    //
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DataSet<String> text = env.readTextFile("data/user.txt");
        DataSet<EmailInfo> mapResult = text.map(new EmailMap());

        try {
            mapResult.count();
            mapResult.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

        UnsortedGrouping<EmailInfo> groupField = mapResult.groupBy("groupField");

        try {
            GroupReduceOperator<EmailInfo, EmailInfo> first = groupField.first(1);
            first.print();
        } catch (Exception e) {
            e.printStackTrace();
        }


        DataSet<EmailInfo> reduceResult = mapResult.groupBy("groupField").reduce(new EmailReduce());

        try {
            List<EmailInfo> collect = reduceResult.collect();

            for (EmailInfo item : collect) {
                String emailType = item.getEmailType();
                Long count = item.getCount();

                System.out.println(emailType + "=" + count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
