package com.natural.data.analyze.spark.customer.analyze;

import com.natural.data.analyze.core.config.ConfigurationManager;
import com.natural.data.analyze.core.constant.Constants;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

public class Context {

    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            return new SQLContext(sc);
        }
        return null;
    }
}
