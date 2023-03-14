package cn.com.businessmatrix.utils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

public class SparkSplitUtils {

    public static final BigDecimal BD_UNKNOWN = new BigDecimal(-1);

    public static final String S_UNKNOWN = "*";

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static int getCateCode(String s) {
        if (s == null || "*".equals(s))
            return 1;
        else
            return 2;
    }

    public static int getCateCode(BigDecimal s) {
        if (s == null || s.longValue() == -1L)
            return 1;
        else
            return 2;
    }

    public static boolean matchRule(BigDecimal val, BigDecimal ruleval) {
        if (nvl(ruleval).longValue() == -1L)
            return true;
        else
            return nvl(val).compareTo(nvl(ruleval)) == 0;
    }

    public static boolean matchRule(String val, String ruleval) {
        if (nvl(ruleval).equals("*"))
            return true;
        else
            return nvl(val).equals(nvl(ruleval));
    }

    public static BigDecimal nvl(BigDecimal bd) {
        if (bd == null)
            return BD_UNKNOWN;
        else
            return bd;
    }

    public static BigDecimal nvl(BigDecimal bd, BigDecimal def) {
        if (bd == null)
            return def;
        else
            return bd;
    }

    public static String nvl(String s) {
        if (s == null)
            return S_UNKNOWN;
        else
            return s;
    }

    public static List<Row> reorder(Map<String, List<Row>> map) {
        List<Row> vt_area_split_cmn = null;
        List<Row> vt_tmp_ar_splt_cmn = null;
        Iterator<String> it = map.keySet().iterator();
        int v_count2, v_cnt1, v_cnt2, v_cnt3;
        while (it.hasNext()) {
            //--取当前拆分规则集
            String itKey = it.next();
            List<Row> vt_cur_ar_splt_cmn = map.get(itKey);
            if (vt_cur_ar_splt_cmn == null) {
                //System.out.println("vc_key==="+itKey);
                continue;
            }
            int v_count1 = vt_cur_ar_splt_cmn.size(); //--当前拆分规则集数量
            if (vt_area_split_cmn != null) {
                vt_tmp_ar_splt_cmn = new ArrayList<Row>();
                //--合并上条拆分规则集和当前拆分规则集
                v_count2 = vt_area_split_cmn.size(); //--上条拆分规则集数量
                v_cnt1 = 0;// --当前拆分规则集下标
                v_cnt2 = 0; //--上条拆分规则集下标
                v_cnt3 = 0; //--合并排序规则集下标
                //--对相邻拆分规则集（每个规则集均已按优先级排序）按优先级从高到低配对对比，保留高的规则
                while (v_cnt1 < v_count1 && v_cnt2 < v_count2) {
                    if (
                            ((BigDecimal) vt_cur_ar_splt_cmn.get(v_cnt1).getAs("priority")).compareTo(
                                    ((BigDecimal) vt_area_split_cmn.get(v_cnt2).getAs("priority"))
                            ) < 0
                    ) {

                        vt_tmp_ar_splt_cmn.add(vt_cur_ar_splt_cmn.get(v_cnt1));
                        v_cnt1 = v_cnt1 + 1;
                    } else {
                        vt_tmp_ar_splt_cmn.add(vt_area_split_cmn.get(v_cnt2));
                        v_cnt2 = v_cnt2 + 1;
                    }
                    //--检查是否存在重载规则
                    v_cnt3 = v_cnt3 + 1;
                }

                //--当前拆分规则集剩余规则直接保留到合并规则集的尾部
                while (v_cnt1 < v_count1) {
                    vt_tmp_ar_splt_cmn.add(vt_cur_ar_splt_cmn.get(v_cnt1));
                    v_cnt3 = v_cnt3 + 1;
                    v_cnt1 = v_cnt1 + 1;
                }

                //--上次拆分规则集剩余规则直接保留到合并规则集的尾部
                while (v_cnt2 < v_count2) {
                    vt_tmp_ar_splt_cmn.add(vt_area_split_cmn.get(v_cnt2));
                    //--检查是否存在重载规则
                    v_cnt3 = v_cnt3 + 1;
                    v_cnt2 = v_cnt2 + 1;
                }

                //复制合并排序规则集
                vt_area_split_cmn = vt_tmp_ar_splt_cmn;
            } else { // --直接复制第一个拆分规则集
                vt_area_split_cmn = vt_cur_ar_splt_cmn;
            }
        }//end while
        if (vt_area_split_cmn == null)
            vt_area_split_cmn = new ArrayList<Row>();
        return vt_area_split_cmn;
    }

    //priority必须唯一
    public static Map<Integer, Row> reOrderUniqe(Map<String, List<Row>> map) {
        TreeMap<Integer, Row> retMap = new TreeMap<Integer, Row>();
        Iterator<String> it = map.keySet().iterator();
        List<Row> ls = null;
        Row r = null;
        while (it.hasNext()) {
            ls = map.get(it.next());
            if (ls != null) {
                for (int i = 0; i < ls.size(); i++) {
                    r = ls.get(i);
                    retMap.put((Integer.parseInt(r.getAs("priority").toString())), r);
                }
            }
        }
        return retMap;
    }

    public static Row gr(Row r, StructType st) {
        return new GenericRowWithSchema(scala.collection.JavaConversions
                .seqAsJavaList(r.toSeq()).toArray(), st);
    }

    //用作本地调试
    public static int addDays(int date, int days) {
        try {
            return Integer.parseInt(SparkSplitUtils.sdf.format(new Date(SparkSplitUtils.sdf.parse(String.valueOf(date)).getTime() + days * 3600 * 24 * 1000L)));
        } catch (NumberFormatException e) {

            e.printStackTrace();
        } catch (ParseException e) {

            e.printStackTrace();
        }
        return 0;
    }
}
