package cn.com.tool.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCUtil {
    static String mysqlJdbcUrl = "jdbc:mysql://47.100.205.207:3306/yz_test";
    static String mysqlUsername = "mysql_root";
    static String mysqlPassword = "Luyz@1024";
    static String mysqlTableName = "y_user";
    static String mysqlDriver = "com.mysql.cj.jdbc.Driver";

    public static Connection getConnection() {
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
