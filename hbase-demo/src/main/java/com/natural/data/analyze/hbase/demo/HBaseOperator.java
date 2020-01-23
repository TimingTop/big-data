package com.natural.data.analyze.hbase.demo;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;

public class HBaseOperator {

    public void createTable(Connection connection, String tableName, String ...columnFamilies) throws IOException {
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

    public void deleteTable(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tname = TableName.valueOf(tableName);

        if (admin.tableExists(tname)) {
            admin.disableTable(tname);
            admin.deleteTable(tname);
        }
    }



    public void addRow(Connection connection, String tableName, String family, Map<String, Object> params) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        Table table = connection.getTable(tname);
        // 属性中 row  = rowkey
        Put put = new Put(params.get("row").toString().getBytes());

        for (Map.Entry<String, Object> m : params.entrySet()) {
            if (m.getKey().equals("row")) {
                continue;
            }

            put.addColumn(family.getBytes(), m.getKey().getBytes(), m.getValue().toString().getBytes());
        }
        table.put(put);
        table.close();
    }

}
