package com.natural.data.analyze.core.db;

import com.natural.data.analyze.core.config.ConfigurationManager;
import com.natural.data.analyze.core.constant.Constants;

import java.sql.*;
import java.util.LinkedList;

public class JDBCHelper {
    private static JDBCHelper instance = null;

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> datasource = new LinkedList<>();

    private JDBCHelper() {
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static interface  QueryCallback {
        void process(ResultSet rs) throws Exception;
    }

}
