package com.natural.data.analyze.core.db;

import com.natural.data.analyze.core.config.ConfigurationManager;
import com.natural.data.analyze.core.constant.Constants;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariHelper {
    private static HikariHelper instance = null;

    private HikariDataSource dataSource = null;

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static HikariHelper getInstance() {
        if (instance == null) {
            synchronized (HikariHelper.class) {
                if (instance == null) {
                    instance = new HikariHelper();
                }
            }
        }
        return instance;
    }

    private HikariHelper() {
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }




}
