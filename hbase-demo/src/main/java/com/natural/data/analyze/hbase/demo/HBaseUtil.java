package com.natural.data.analyze.hbase.demo;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {

    /**
     * create table
     * @param tableName
     * @param columnFamily
     * @return
     * @throws IOException
     */
    public static Boolean createTable(String tableName, List<String> columnFamily) throws IOException {

        Admin admin = HBaseConn.getHBaseConnection().getAdmin();

        List<ColumnFamilyDescriptor> familyDescriptors
                = new ArrayList<>(columnFamily.size());

        columnFamily.forEach(cf -> {
            familyDescriptors.add(
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build()
            );
        });

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamilies(familyDescriptors)
                .build();

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table Exists!");
        } else {
            admin.createTable(tableDescriptor);

        }

        return true;
    }

    public static boolean createTableBySplitKeys(String tableName, List<String> columnFamily, byte[][] splitKeys) throws IOException {
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        } else {
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(columnFamily.size());

            columnFamily.forEach(cf -> {
                familyDescriptors.add(
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build()
                );
            });

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(familyDescriptors)
                    .build();

            admin.createTable(tableDescriptor, splitKeys);
        }

        return true;
    }

    public byte[][] getSplitKeys(String[] keys ) {
        if (keys == null) {
            keys = new String[] {
                   "1|", "2|", "3|", "4|", "5|", "6|",
                   "7|", "8|", "9|"
            };
        }
        byte[][] splitKeys = new byte[keys.length][];

        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(String key : keys) {
            rows.add(Bytes.toBytes(key));
        }

        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;



    }

    public Table getTable(String tableName) throws IOException {
        return HBaseConn.getHBaseConnection().getTable(TableName.valueOf(tableName));
    }

    public List<String> getAllTableNames() throws IOException {
        List<String> result = new ArrayList<>();
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();
        TableName[] tableNames = admin.listTableNames();

        for(TableName tableName : tableNames) {
            result.add(tableName.getNameAsString());
        }
        return result;
    }

    private Map<String, Map<String, String>> queryData(String tableName, Scan scan) throws IOException {
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultScanner rs = null;

        Table table = null;

        table = getTable(tableName);
        rs = table.getScanner(scan);

        for (Result r : rs) {
            Map<String, String> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : r.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierLength(), cell.getQualifierLength()),
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            if (rowKey != null) {
                result.put(rowKey, columnMap);
            }
        }

        return result;
    }

    public Map<String, Map<String, String>> getResultScanner(String tableName) throws IOException {
        Scan scan = new Scan();
        return queryData(tableName, scan);
    }

    public Map<String, Map<String, String>> getResultScanner(String tableName, String startRowKey, String stopRowKey) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(stopRowKey));


        return queryData(tableName, scan);
    }

    public Map<String, Map<String, String>> getResultScannerPrefixFilter(String tableName, String prefix) throws IOException {
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
        scan.setFilter(filter);
        return queryData(tableName, scan);
    }

    public Map<String, Map<String, String>> getResultScannerRowFilter(String tableName, String keyword) throws IOException {
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL,new SubstringComparator(keyword));
        scan.setFilter(filter);

        return queryData(tableName, scan);

    }
    // 返回一行数据
    public Map<String, String> getRowData(String tableName, String rowKey) throws IOException {
        Map<String, String> result = new HashMap<>();
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = getTable(tableName);
        Result hTableResult = table.get(get);

        if (hTableResult != null && !hTableResult.isEmpty()) {
            for (Cell cell : hTableResult.listCells()) {
                result.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getQualifierLength()));

            }
        }

        return result;

    }

    public String getColumnValue(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        String str = null;
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = getTable(tableName);
        Result result = table.get(get);
        if (result != null && !result.isEmpty()) {
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            if (cell != null) {
                str = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return str;

    }

    public List<String> getColumnValueByVersion(String tableName, String rowKey, String familyName, String columnName, int versions) throws IOException {
        List<String> result = new ArrayList<>(versions);
        Table table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.readVersions(versions);
        Result hTableResult = table.get(get);
        if (hTableResult != null && !hTableResult.isEmpty()) {
            for (Cell cell : hTableResult.listCells()) {
                result.add(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

        }

        return result;
    }

    // 更新，添加数据
    public void putData(String tableName, String rowKey, String familyName, String[] columns, String[] values ) throws IOException {
        Table table = getTable(tableName);
        putData(table, rowKey, tableName, familyName, columns, values);
    }

    public void putData(Table table, String rowKey, String tableName, String familyName, String[] columns, String[] values) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null && values[i] != null) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));

            } else {

            }
        }

        table.put(put);
        table.close();
    }

    public void setColumnValue(String tableName, String rowKey, String familyName, String column1, String value1) throws IOException {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1), Bytes.toBytes(value1));

        table.put(put);

    }

    public boolean deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();
        Table table = null;
        if (admin.tableExists(TableName.valueOf(tableName))) {
            table = getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            table.delete(delete);
            return true;
        }
        return false;
    }

    public boolean deleteRow(String tableName, String rowKey) throws IOException {
        Table table = null;
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();

        if (admin.tableExists(TableName.valueOf(tableName))) {
            table = getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            return true;
        }
        return false;
    }

    public boolean deleteColumnFamily(String tableName, String columnFamily) throws IOException {
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.deleteColumnFamily(TableName.valueOf(tableName),
                    Bytes.toBytes(columnFamily));
            return true;
        }
        return false;
    }

    public boolean deleteTable(String tableName) throws IOException {
        Admin admin = HBaseConn.getHBaseConnection().getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        }
        return false;

    }

    private void close(Admin admin, ResultScanner rs, Table table) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (rs != null) {
            rs.close();
        }
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static class PriorityTTL {
        private String[] keys;
        private long TTL;

        public String[] getKeys() {
            return keys;
        }

        public void setKeys(String[] keys) {
            this.keys = keys;
        }

        public long getTTL() {
            return TTL;
        }

        public void setTTL(long TTL) {
            this.TTL = TTL;
        }
    }

    public static void main(String[] args) {
        String aa = "[{\"keys\": [1,2,3], \"TTL\": 1000}, {\"keys\": [4,5,6]," +
                "    \"TTL\": 2000}]";

        List<PriorityTTL> priorityTTLs = JSONArray.parseArray(aa, PriorityTTL.class);

        System.out.println("a");

    }

}
