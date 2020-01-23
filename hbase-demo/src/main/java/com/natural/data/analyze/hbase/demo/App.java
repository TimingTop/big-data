package com.natural.data.analyze.hbase.demo;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class App {
    /**
     * v 1.9.0
     * @param args
     */
    public static void main(String[] args) throws IOException {
        Connection conn = HBaseConn.getHBaseConnection();

        Admin admin = conn.getAdmin();
        // 创建表结构
        String tableName = "student";
        String[] family = {"school", "family"};
//        createTable( conn, tableName, "school", "family");

        HBaseOperator operator = new HBaseOperator();
        Map<String, Object> input = new HashMap<>();

        input.put("row", "row001");
        input.put("class", "class-2");
        input.put("no", "00001");
        input.put("age", "6");

        operator.addRow(conn, tableName, "school", input);

        System.out.println("insert into a  row");


    }

    public static void createTable(Connection connection, String tableName, String ...columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tname = TableName.valueOf(tableName);
        if (admin.tableExists(tname)) {
            System.out.println("Table exist : " + tableName);
        } else {
            TableDescriptorBuilder tdesc = TableDescriptorBuilder.newBuilder(tname);
            for (String column : columnFamilies) {
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(column);
                tdesc.setColumnFamily(cfd);
            }
            TableDescriptor desc = tdesc.build();
            admin.createTable(desc);
        }
    }
}
