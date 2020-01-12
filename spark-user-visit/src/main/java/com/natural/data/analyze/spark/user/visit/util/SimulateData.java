package com.natural.data.analyze.spark.user.visit.util;

import com.natural.data.analyze.core.util.DateUtils;
import com.natural.data.analyze.core.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class SimulateData {

    public static void simulation(SQLContext spark) {

        List<Row> rows = new ArrayList<>();

        String[] searchKeywords = new String[]{
                "pineapple", "blueberry", "apple", "pomegranate", "mango",
                "lemon", "durian",  "watermelon", "olive", "orange",
                "banana", "guava", "cherry"
        };

        String[] actions = new String[]{"search", "click", "order", "pay"};
        // yyyy-MM-dd
        String date = DateUtils.getTodayDate();
        Random random = new Random();

        for(int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);

            for(int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                Long clickCategoryId = null;

                for(int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = date +
                            " " + StringUtils.fulfill(String.valueOf(random.nextInt(23))) +
                            ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59))) +
                            ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(13)];
                    } else if("click".equals(action)) {
                        if(clickCategoryId == null) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));
                    rows.add(row);
                }
            }
        }

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.LongType, true)
        ));

        Dataset<Row> userVisitSession = spark.createDataFrame(rows, schema);

        // https://spark.apache.org/docs/latest/sql-getting-started.html
        userVisitSession.createOrReplaceTempView("user_visit_action");
//        userVisitSession.show(2);

        /**
         * ==========
         */
        rows.clear();

        String[] sexes = new String[]{"male", "female"};
        for(int i = 1; i < 100; i++){
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age, professional, city, sex);
            rows.add(row);
        }

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)
        ));

        Dataset<Row> userInfoDS = spark.createDataFrame(rows, schema2);
        userInfoDS.createOrReplaceTempView("user_info");
//        userInfoDS.show(2);

        /*
        ====================
         */
        rows.clear();

        int[] productStatus = new int[]{0, 1};

        for(int i = 0; i < 100; i++) {
            long productId = i;
            String productName = "product" + i;
            String extendInfo = "{\"product_status\":" + productStatus[random.nextInt(2)] + "}";
            Row row = RowFactory.create(productId, productName, extendInfo);
            rows.add(row);
        }

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)
        ));

        Dataset<Row> productInfoDS = spark.createDataFrame(rows, schema3);
        productInfoDS.createOrReplaceTempView("product_info");
//        productInfoDS.show(2);

    }
}
