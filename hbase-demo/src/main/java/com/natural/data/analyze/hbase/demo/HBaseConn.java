package com.natural.data.analyze.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HBaseConn() {
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "192.168.56.104:2181,192.168.56.105:218,192.168.56.106:2181");
        }
    }

    private Configuration getConfiguration() {
        return configuration;
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHBaseConnection() {
        return INSTANCE.getConnection();
    }

    public static Configuration getHBaseConfiguration() {
        return INSTANCE.getConfiguration();
    }

    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
