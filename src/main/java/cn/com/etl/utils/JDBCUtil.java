package cn.com.etl.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class JDBCUtil {

    static String mysqlDriver = "com.mysql.cj.jdbc.Driver";

    public static Connection getConnection() {
        Map<String, String> getConfig = GetConfig.getconfig();
        String mysqlUsername = getConfig.get("oracleUserName");
        String mysqlPassword = getConfig.get("oraclePassWord");
        String mysqlJdbcUrl = getConfig.get("oracleUrl");
        Connection conn = null;
        try {
            Class.forName(mysqlDriver);
            conn = DriverManager.getConnection(mysqlJdbcUrl, mysqlUsername, mysqlPassword);
            //关闭数据库事务的自动提交，默认值为true
            conn.setAutoCommit(false);
        } catch (
                SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;

    }

    public static Connection getConnection(String sql) {
        Map<String, String> getConfig = GetConfig.getconfig();
        String mysqlUsername = getConfig.get("oracleUserName");
        String mysqlPassword = getConfig.get("oraclePassWord");
        String mysqlJdbcUrl = getConfig.get("oracleUrl");
        Connection conn = null;
        try {
            Class.forName(mysqlDriver);
            conn = DriverManager.getConnection(mysqlJdbcUrl, mysqlUsername, mysqlPassword);
            //关闭数据库事务的自动提交，默认值为true
            conn.setAutoCommit(false);
        } catch (
                SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;

    }

}
