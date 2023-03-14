package cn.com.test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

public class Test {

    public static void main(String[] args) {


        double ret = Double.parseDouble(new DecimalFormat("#.9999999").format(-629843534534535.53333d));
        System.out.println(ret);

        BigDecimal bd = new BigDecimal(99.904d, new MathContext(2));
        bd.setScale(4);

        System.out.println("20210101".compareTo("20210102"));
        if (true)
            return;
        String x = null;

        System.out.println(true ? "ddd" : new BigDecimal(x));

        String transferInTypes = "(127)";//args[7];//转托管入，非交易过户入
        String transferOutTypes = "(128,199,10135)";//args[8];//转托管出，非交易过户出

        List<String> transferTypesList = new ArrayList(Arrays.asList(transferInTypes.replace("(", "").replace(")", "").split(",")));
        //transferTypesList.addAll(Arrays.asList(transferOutTypes.replace("(", "").replace(")","").split(",")));

        System.out.println(StringUtils.join(transferTypesList, "','"));


        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            System.out.println(sdf.format(new Date(sdf.parse("20200531")
                    .getTime() + 1 * 3600 * 24 * 1000L)));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        String s1 = "C7E0-BAA3-CAA1-CEF7-C4FE-CAD0-B1B1-B4F3-BDD6-B531-3223-CAA1-D2A9-C6B7-BCEC-D1E9-CBF9";
        // String
        // s1="C7E0-BAA3-CAA1-CEF7-C4FE-CAD0-B1B1-B4F3-BDD6-A3BF-3223-CAA1-D2A9-C6B7-BCEC-D1E9-CBF9";
        String s2 = s1.replaceAll("-", "");
        byte[] b = new byte[s2.length() / 2];
        for (int i = 0; i < s2.length() / 2; i++) {
            // System.out.println(s2.substring(2*i,2*(i+1)));
            b[i] = Integer.valueOf(s2.substring(2 * i, 2 * (i + 1)), 16)
                    .byteValue();

        }
        System.out.println(Hex.encodeHexString(b));
        // System.out.println(new String(b));
        try {
            System.out.println(Hex.encodeHexString(new String(b, "gb2312").getBytes("utf8")));
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        TreeMap<String, String> retMap = new TreeMap<String, String>();

        retMap.put("1", "a");
        retMap.put("2", "b");
        retMap.put("1", "d");
        retMap.put("1" + "", "c");
        Iterator<String> it = retMap.keySet().iterator();
        while (it.hasNext()) {

            // it.next();
            System.out.println(retMap.get(it.next()));

        }

        System.out.println("n" + null);

        String[] s = "yhods./user/hive/warehouse/fdp/yhods.db/tmp_trd_st_assoc_area_1/*".split("/");

        System.out.println(s[0] + s[s.length - 2]);

        //BigDecimal bd=new BigDecimal(17.05d);
        System.out.println(bd + "-");
        //System.out.println(cn.com.businessmatrix.utils.SparkSplitUtils.addDays(20191122,185));
    }

}
