package com.natural.data.analyze.flink.portrait.util;


import org.junit.Test;

import java.util.List;

public class IKUtilTest {

    @Test
    public void test01() {
        String aa = "今天的天气非常好";

        List<String> ikWord = IKUtil.getIKWord(aa);

        for (String item : ikWord) {
            System.out.println("=====" + item + "=========");
        }
    }
}