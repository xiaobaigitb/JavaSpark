package cn.com.businessmatrix.domain;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;

/**
 * 根据json文件解析txt文件
 */


public class DataItem {
    public String fieldName;
    public String dataType;
    public String preDateFormat;
    public String postDateFormat;
    public int scale;
    public int precision;

    public DataItem() {
    }

    DataItem(JSONObject json) {
        this.fieldName = json.getString("fieldName");
        this.dataType = json.getString("dataType");
        this.scale = json.getString("scale") != null && json.getString("scale").length() != 0 ? json.getInt("scale") : 0;
        this.precision = json.getString("precision") != null && json.getString("precision").length() != 0 ? json.getInt("precision") : 0;
        this.preDateFormat = json.getString("preDateFormat");
        this.postDateFormat = json.getString("postDateFormat");
    }

    public static Map<String, DataItem> fromJsonDataDict(String fileName) {
        StringBuffer sb = new StringBuffer();

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line = null;

            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            br.close();
        } catch (IOException var6) {
            var6.printStackTrace();
        }

        Map<String, DataItem> map = new HashMap();
        JSONArray ja = JSONArray.fromObject(sb.toString());
        JSONObject json = null;

        for (int i = 0; i < ja.size(); ++i) {
            json = ja.getJSONObject(i);
            map.put(json.getString("fieldName"), new DataItem(json));
        }

        return map;
    }

    public static DataItem[] fromJsonConfig(String fileName) {
        StringBuffer sb = new StringBuffer();

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line = null;

            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            br.close();
        } catch (IOException var6) {
            var6.printStackTrace();
        }

        JSONArray ja = JSONArray.fromObject(sb.toString());
        DataItem[] items = new DataItem[ja.size()];
        JSONObject json = null;

        for (int i = 0; i < ja.size(); ++i) {
            json = ja.getJSONObject(i);
            items[i] = new DataItem(json);
        }

        return items;
    }

    public static String rtrim(String str) {
        int num = str.length();

        for (int i = num - 1; i > -1; --i) {
            if (!str.substring(i, i + 1).equals(" ")) {
                return str.substring(0, i + 1);
            }
        }

        return str.trim();
    }

    public static void main(String[] args) {
        String val = "0100030000030000000";
        String retStr = null;
        String mid = val.substring(0, val.length() - 8) + "." + val.substring(val.length() - 8);
        if (Long.parseLong(val.trim()) == 0L) {
            retStr = "0";
        } else {
            retStr = (new BigDecimal(mid.trim())).toString();
        }

        System.out.println(retStr);
    }

    public String toString() {
        return "DataItem{fieldName='" + this.fieldName + '\'' + ", dataType='" + this.dataType + '\'' + ", preDateFormat='" + this.preDateFormat + '\'' + ", postDateFormat='" + this.postDateFormat + '\'' + ", scale=" + this.scale + ", precision=" + this.precision + '}';
    }

    public String formatDate(String val, String preDateFormat, String postDateFormat) {
        if (val != null && !val.isEmpty() && preDateFormat != null && !preDateFormat.isEmpty() && postDateFormat != null && !postDateFormat.isEmpty()) {
            SimpleDateFormat pre = new SimpleDateFormat(preDateFormat);
            SimpleDateFormat post = new SimpleDateFormat(postDateFormat);

            try {
                return post.format(pre.parse(val));
            } catch (ParseException var7) {
                var7.printStackTrace();
            }
        }

        return val;
    }

    public String parse(String val) {
        String retStr = null;
        if ("C".equals(this.dataType)) {
            retStr = rtrim(val);
        } else if ("A".equals(this.dataType)) {
            retStr = rtrim(val);
        } else if ("N".equals(this.dataType)) {
            String mid = val.substring(0, val.length() - this.precision) + (this.precision > 0 ? "." : "") + val.substring(val.length() - this.precision);
            if (Long.parseLong(val.trim()) == 0L) {
                retStr = "0";
            } else {
                retStr = (new BigDecimal(mid.trim())).toString();
            }
        } else {
            retStr = val;
        }

        retStr = this.formatDate(retStr, this.preDateFormat, this.postDateFormat);
        return StringUtils.trim(retStr);
    }
}
