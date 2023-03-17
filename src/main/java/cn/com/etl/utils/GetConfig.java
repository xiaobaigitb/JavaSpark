package cn.com.etl.utils;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class GetConfig {

    public static void main(String[] args) {
        Map<String, String> getconfig = GetConfig.getconfig();
        for (String str :
                getconfig.keySet()) {
            System.out.println(str+ " : " + getconfig.get(str));
        }
    }

    /*获取配置文件的第二种方式,ResourceBundle:中文 资源捆绑 getBundle:获取捆绑
     * 不用带文件后缀名
     * */
    public static Map<String, String> getconfig() {
        Map<String, String> stringMap = new HashMap<String, String>();

        ResourceBundle properties = ResourceBundle.getBundle("config");
        String oracleUserName = properties.getString("oraclecon.username");
        //String oraclePassWord = base64ToString(properties.getString("oraclecon.password"));
        String oraclePassWord = properties.getString("oraclecon.password");
        String oracleUrl = properties.getString("oraclecon.url");
        String oracleUrl2 = properties.getString("oraclecon.url2");
        stringMap.put("oracleUserName", oracleUserName);
        stringMap.put("oraclePassWord", oraclePassWord);
        stringMap.put("oracleUrl", oracleUrl);
        stringMap.put("oracleUrl2", oracleUrl2);
        return stringMap;
    }

    static String base64ToString(String base64Str) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Str);
        String decodedString = new String(decodedBytes);
        return decodedString;
    }

}
