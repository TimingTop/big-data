package com.natural.data.analyze.core.util;

import com.natural.data.analyze.core.config.ConfigurationManager;
import com.natural.data.analyze.core.constant.Constants;
import org.apache.spark.SparkConf;

public class SparkUtils {

    public static void setMaster(SparkConf conf) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }
}
