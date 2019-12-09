package com.natural.data.analyze.flink.portrait.task;

import com.natural.data.analyze.flink.portrait.entity.BaiJiaInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



// id,productid,
public class BaijiaTask {
    public static void main(String[] args) {

        ParameterTool params = ParameterTool.fromArgs(args);
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("localhost", 9990);
        
        text.map(new MapFunction<String, BaiJiaInfo>() {
            @Override
            public BaiJiaInfo map(String s) throws Exception {
                if (StringUtils.isBlank(s)) {
                    return null;
                }

                String[] orderinfos = s.split(",");

                String id= orderinfos[0];
                String productid = orderinfos[1];
                String producttypeid = orderinfos[2];
                String createtime = orderinfos[3];
                String amount = orderinfos[4];
                String paytype = orderinfos[5];
                String paytime = orderinfos[6];
                String paystatus = orderinfos[7];
                String couponamount = orderinfos[8];
                String totalamount = orderinfos[9];
                String refundamount = orderinfos[10];
                String num = orderinfos[11];
                String userid = orderinfos[12];

                BaiJiaInfo baiJiaInfo = new BaiJiaInfo();
                baiJiaInfo.setUserid(userid);
                baiJiaInfo.setCreatetime(createtime);


                return baiJiaInfo;
            }
        })
        
    }
}
