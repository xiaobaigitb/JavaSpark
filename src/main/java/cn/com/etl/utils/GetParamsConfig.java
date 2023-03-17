package cn.com.etl.utils;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class GetParamsConfig {
    public static void main(String[] args) {
        Map<String, Object> objectMap = GetParamsConfig.getConfig("Y");
        for (Object key :
                objectMap.keySet()) {
            System.out.println(key.toString() + " : " + objectMap.get(key));
        }
    }

    static Map<String, Object> getConfig(String isGlobal) {
        Connection connection = JDBCUtil.getConnection();
        //自定义变量
        Map<String, Object> argvType = new HashMap<>();
        //全局变量
        Map<String, Object> globalDict = new HashMap<>();

        try {
            Statement statement = connection.createStatement();
            String sql = "select params_name, params_type, isglobal, params_value from ctl_params_config";
            ResultSet resultSet = statement.executeQuery(sql);
            connection.commit();

            ResultSetMetaData md = resultSet.getMetaData();//获取键名
            int columnCount = md.getColumnCount();//获取列的数量
            while (resultSet.next()) {
                Map<String, Object> rowData = new HashMap<>();//声明Map
                if ("N".equals(resultSet.getString(3))) {
                    argvType.put(resultSet.getString(1), resultSet.getObject(2));
                } else {
                    globalDict.put(resultSet.getString(1), resultSet.getObject(4));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();//事务的回滚
                System.out.println("事务的回滚");
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        if ("N".equals(isGlobal)) {
            return argvType;
        } else {
            return globalDict;
        }
    }
}
