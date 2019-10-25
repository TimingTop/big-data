package com.natural.data.analyze.hbase.demo;

import org.apache.hadoop.hbase.client.Connection;

public class App {
    /**
     * v 1.9.0
     * @param args
     */
    public static void main(String[] args) {
        Connection conn = HBaseConn.getHBaseConnection();


    }
}
