package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import cn.com.businessmatrix.utils.SparkSplitUtils;

public class SparkAreaSplit4 {

    /*
     * 管理区划拆分---本地调试模式
     */

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.set("spark.master", "local");
        conf.setAppName("debugspark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Row> dataList = Arrays.asList(initData());

        Row[] rulerows = new Row[3];
        initRules(rulerows);

        Map<String, List<Row>> ruleMap = new HashMap<String, List<Row>>();
        List<Row> ls = null;
        String key = null;
        for (int i = 0; i < rulerows.length; i++) {
            key = rulerows[i].getString(0);
            ls = ruleMap.get(key);
            if (ls == null)
                ls = new ArrayList<Row>();
            ls.add(rulerows[i]);
            ruleMap.put(key, ls);
        }

        List<String> w = new ArrayList<String>();
        for (int i = 0; i < dataList.size(); i++) {
            Row r1 = dataList.get(i);
            List<Row> rs = x(r1, ruleMap);
            for (int j = 0; j < rs.size(); j++) {
                Row r2 = rs.get(j);
                w.add(r2.getAs(0).toString() + "," + ((BigDecimal) r2.getAs(8)).setScale(2, BigDecimal.ROUND_UP) + "," + r2.getAs(7));
            }
            //System.out.println(w.size());
        }


        sc.parallelize(w).saveAsTextFile("/tmp/12346");
        sc.close();
    }

    static List<Row> x(Row r, Map<String, List<Row>> ruleMap) {

        int v_cnt0, v_cnt1, v_cnt2, v_cnt3, v_cnt4, v_cnt5, v_cnt6;
        int v_count0, v_count1, v_count2, v_count3, v_count4, v_count5, v_count6;

        String vc_key = null;
        List<Row> ret = new ArrayList<Row>();

        Map<String, List<Row>> innerMap = new HashMap<String, List<Row>>();

        String pi_cserialno = (String) r.getAs("cserialno");
        String pi_tano = (String) r.getAs("dk_tano");
        String pi_dk_agency_type = (String) r.getAs("dk_agency_type");
        String pi_dk_cust_type = (String) r.getAs("dk_cust_type");
        String pi_agencyno = (String) r.getAs("agencyno");
        String pi_netno = (String) r.getAs("netno");
        String pi_dk_share_type = (String) r.getAs("dk_share_type");

        BigDecimal vc_pre_rule_id = null;
        BigDecimal vc_pre_priority = null;
        String vc_pre_dync_flag = null;

        BigDecimal pi_sk_prod_type = (BigDecimal) r.getAs("sk_product_type");
        BigDecimal pi_sk_product = (BigDecimal) r.getAs("sk_product");
        BigDecimal pi_sk_invpty_type = (BigDecimal) r.getAs("sk_invpty_type");
        BigDecimal pi_sk_invpty = (BigDecimal) r.getAs("sk_invpty");
        BigDecimal pi_sk_account_type = (BigDecimal) r.getAs("sk_account_type");
        BigDecimal pi_sk_account = (BigDecimal) r.getAs("sk_account");
        BigDecimal pi_sk_tradeacco_reg = (BigDecimal) r.getAs("sk_tradeacco_reg");
        BigDecimal pi_sk_date = (BigDecimal) r.getAs("sk_date");

        BigDecimal pi_lval_ag = (BigDecimal) r.getAs("lval_ag");
        BigDecimal pi_rval_ag = (BigDecimal) r.getAs("rval_ag");
        BigDecimal pi_lval_rg = (BigDecimal) r.getAs("lval_rg");
        BigDecimal pi_rval_rg = (BigDecimal) r.getAs("rval_rg");
        BigDecimal pi_sk_agency_of_lv1 = (BigDecimal) r.getAs("sk_agency_of_lv1");
        BigDecimal pi_sk_region_of_lv1 = (BigDecimal) r.getAs("sk_region_of_lv1");

        if (pi_cserialno == null || "*".equals(pi_cserialno))
            v_count0 = 1;
        else
            v_count0 = 2;

        if (pi_sk_account == null || pi_sk_account.intValue() == -1)
            v_count1 = 1;
        else
            v_count1 = 2;

        if (pi_sk_invpty == null || pi_sk_invpty.intValue() == -1)
            v_count2 = 1;
        else
            v_count2 = 2;

        if (pi_dk_cust_type == null || "*".equals(pi_cserialno))
            v_count3 = 1;
        else
            v_count3 = 2;

        if (pi_sk_product == null || pi_sk_product.intValue() == -1)
            v_count4 = 1;
        else
            v_count4 = 2;

        if (pi_sk_agency_of_lv1 == null || pi_sk_agency_of_lv1.intValue() == -1)
            v_count5 = 1;
        else
            v_count5 = 2;

        if (pi_sk_region_of_lv1 == null || pi_sk_region_of_lv1.intValue() == -1)
            v_count6 = 1;
        else
            v_count6 = 2;

        for (v_cnt0 = v_count0; v_cnt0 >= 1; v_cnt0--) {
            for (v_cnt1 = v_count1; v_cnt1 >= 1; v_cnt1--) {
                for (v_cnt2 = v_count2; v_cnt2 >= 1; v_cnt2--) {
                    for (v_cnt3 = v_count3; v_cnt3 >= 1; v_cnt3--) {
                        for (v_cnt4 = v_count4; v_cnt4 >= 1; v_cnt4--) {
                            for (v_cnt5 = v_count5; v_cnt5 >= 1; v_cnt5--) {
                                for (v_cnt6 = v_count6; v_cnt6 >= 1; v_cnt6--) {
                                    vc_key = (v_cnt0 == 1 ? "*.*" : pi_tano + '.' + pi_cserialno) + ',' +
                                            (v_cnt1 == 1 ? -1 : pi_sk_account) + ',' +
                                            (v_cnt2 == 1 ? -1 : pi_sk_invpty) + ',' +
                                            (v_cnt3 == 1 ? "*" : pi_dk_cust_type) + ',' +
                                            (v_cnt4 == 1 ? -1 : pi_sk_product) + ',' +
                                            (v_cnt5 == 1 ? -1 : pi_sk_agency_of_lv1) + ',' +
                                            (v_cnt6 == 1 ? -1 : pi_sk_region_of_lv1);
                                    innerMap.put(vc_key, ruleMap.get(vc_key));
                                }
                            }
                        }
                    }
                }
            }
        }// end for

        List<Row> orMap = SparkSplitUtils.reorder(innerMap);
        double v_accu_ratio = 1.0d;
        double v_ratio = 0.0d;
        double v_lv_remain_ratio = 0.0d;
        double v_e = 0.0000001d;
        vc_pre_rule_id = SparkSplitUtils.BD_UNKNOWN;
        Iterator<Row> it = orMap.iterator();

        Row rule = null;
        BigDecimal priority = null;
        BigDecimal area_rule_id = null;
        String dk_anal_dymic = null;
        BigDecimal ratio = null;

        while (it.hasNext()) {
            //System.out.println("=============data========="+it);
            rule = it.next();
            //只取有效日期区间段
            if (
                    pi_sk_date.compareTo((BigDecimal) rule.getAs("effective_from")) >= 0
                            && pi_sk_date.compareTo((BigDecimal) rule.getAs("effective_to")) <= 0
                            && v_accu_ratio > v_e
            ) {
                //按其它拆分维度匹配规则
                if (
                        (pi_cserialno != null && pi_tano != null
                                && pi_cserialno.equals(rule.getAs("cserialno"))
                                && pi_tano.equals(rule.getAs("dk_tano"))
                                || "*".equals(rule.getAs("cserialno"))
                                && "*".equals(rule.getAs("dk_tano")) //跟oracle版本不一样，要去tano和cserialno同时有或无
                        )
                                && pi_lval_ag.compareTo((BigDecimal) rule.getAs("lval_ag")) >= 0
                                && pi_rval_ag.compareTo((BigDecimal) rule.getAs("rval_ag")) <= 0
                                && pi_lval_rg.compareTo((BigDecimal) rule.getAs("lval_rg")) >= 0
                                && pi_rval_rg.compareTo((BigDecimal) rule.getAs("rval_rg")) <= 0
                                && SparkSplitUtils.matchRule(pi_sk_account, (BigDecimal) rule.getAs("sk_account"))
                                && SparkSplitUtils.matchRule(pi_sk_invpty, (BigDecimal) rule.getAs("sk_invpty"))
                                && SparkSplitUtils.matchRule(pi_sk_prod_type, (BigDecimal) rule.getAs("sk_product_type"))
                                && SparkSplitUtils.matchRule(pi_sk_product, (BigDecimal) rule.getAs("sk_product"))
                                && SparkSplitUtils.matchRule(pi_sk_account_type, (BigDecimal) rule.getAs("sk_account_type"))
                                && SparkSplitUtils.matchRule(pi_sk_invpty_type, (BigDecimal) rule.getAs("sk_invpty_type"))
                                && SparkSplitUtils.matchRule(pi_sk_tradeacco_reg, (BigDecimal) rule.getAs("sk_tradeacco_reg"))
                                && SparkSplitUtils.matchRule(pi_dk_agency_type, (String) rule.getAs("dk_agency_type"))
                                && SparkSplitUtils.matchRule(pi_dk_cust_type, (String) rule.getAs("dk_cust_type"))
                                && SparkSplitUtils.matchRule(pi_agencyno, (String) rule.getAs("agencyno"))
                                && SparkSplitUtils.matchRule(pi_netno, (String) rule.getAs("netno"))
                                && SparkSplitUtils.matchRule(pi_dk_share_type, (String) rule.getAs("dk_share_type"))

                ) {
                    priority = (BigDecimal) rule.getAs("priority");
                    area_rule_id = (BigDecimal) rule.getAs("area_rule_id");
                    dk_anal_dymic = (String) rule.getAs("dk_anal_dymic");
                    ratio = (BigDecimal) rule.getAs("ratio");

                    if (
                            SparkSplitUtils.nvl(vc_pre_priority).compareTo(SparkSplitUtils.nvl(priority)) == 0
                                    && SparkSplitUtils.nvl(vc_pre_rule_id).compareTo(SparkSplitUtils.nvl(area_rule_id)) != 0
                                    && "Y".equals(vc_pre_dync_flag)
                                    && "Y".equals(dk_anal_dymic)
                    ) {
                        //相对比例
                        v_ratio = Math.max(v_lv_remain_ratio, v_accu_ratio) * ratio.doubleValue();
                    } else {
                        //绝对比例
                        v_ratio = Math.min(v_accu_ratio, ratio.doubleValue());
                    }
                    ret.add(RowFactory.create(
                            r.getAs(0),//流水号，唯一值
                            area_rule_id,
                            rule.getAs("dk_org_tree"),
                            rule.getAs("sk_org"),
                            rule.getAs("dk_org_tree_of_bl"),
                            rule.getAs("sk_org_of_bl"),
                            rule.getAs("dk_custmngr_type"),
                            rule.getAs("sk_invpty_of_custmngr"),
                            new BigDecimal(v_ratio),
                            pi_sk_date,
                            rule.getAs("lval_ar"),
                            rule.getAs("rval_ar"),
                            rule.getAs("lval_bl"),
                            rule.getAs("rval_bl")
                    ));
                    if (
                        //当层级发生变更时，进行层级剩余比例初始化
                            (vc_pre_priority == null || vc_pre_priority.compareTo(priority) != 0)
                                    && "Y".equals(dk_anal_dymic)
                    ) {
                        //记录同层级的剩余比例
                        v_lv_remain_ratio = v_accu_ratio;
                    }
                    // 记录本次规则的优先级、动态比例计算标志、规则id作为下条规则的基准
                    vc_pre_priority = priority;
                    vc_pre_rule_id = area_rule_id;
                    vc_pre_dync_flag = dk_anal_dymic;
                    v_accu_ratio = v_accu_ratio - v_ratio;
                }//end 多维度拆分
            }// end 有效时间段
            if (v_accu_ratio < v_e) {
                //System.out.println("=============break========="+v_accu_ratio);
                break;
            }
        }// end while

        if (v_accu_ratio > v_e) {
            ret.add(RowFactory.create(
                    r.getAs(0),//流水号，唯一值
                    null,
                    null,
                    new BigDecimal(-1),
                    null,
                    new BigDecimal(-1),
                    null,
                    new BigDecimal(-1),
                    new BigDecimal(v_accu_ratio),
                    pi_sk_date,
                    new BigDecimal(-1),
                    new BigDecimal(999999999999L),//12g9
                    new BigDecimal(-1),
                    new BigDecimal(999999999999L)
            ));
        }
        //System.out.println("=============return========="+ret);
        return ret;
    }

    static void initRules(Row[] r) {
        StructField[] sfs = new StructField[40];
        sfs[0] = new StructField("key", new StringType(), true, null);
        sfs[1] = new StructField("area_rule_id", new DecimalType(12, 0), true, null);
        sfs[2] = new StructField("sk_product_type", new DecimalType(12, 0), true, null);
        sfs[3] = new StructField("sk_product", new DecimalType(12, 0), true, null);
        sfs[4] = new StructField("dk_cust_type", new StringType(), true, null);
        sfs[5] = new StructField("sk_invpty_type", new DecimalType(12, 0), true, null);
        sfs[6] = new StructField("sk_invpty", new DecimalType(12, 0), true, null);
        sfs[7] = new StructField("sk_account_type", new DecimalType(12, 0), true, null);
        sfs[8] = new StructField("sk_account", new DecimalType(12, 0), true, null);
        sfs[9] = new StructField("sk_agency", new DecimalType(12, 0), true, null);
        sfs[10] = new StructField("dk_agency_type", new StringType(), true, null);
        sfs[11] = new StructField("sk_region", new DecimalType(12, 0), true, null);
        sfs[12] = new StructField("dk_org_tree", new StringType(), true, null);
        sfs[13] = new StructField("sk_org", new DecimalType(12, 0), true, null);
        sfs[14] = new StructField("dk_org_tree_of_bl", new StringType(), true, null);
        sfs[15] = new StructField("sk_org_of_bl", new DecimalType(12, 0), true, null);
        sfs[16] = new StructField("dk_custmngr_type", new StringType(), true, null);
        sfs[17] = new StructField("sk_invpty_of_custmngr", new DecimalType(12, 0), true, null);
        sfs[18] = new StructField("priority", new DecimalType(12, 0), true, null);
        sfs[19] = new StructField("ratio", new DecimalType(5, 2), true, null);
        sfs[20] = new StructField("effective_from", new DecimalType(8, 0), true, null);
        sfs[21] = new StructField("effective_to", new DecimalType(12, 0), true, null);
        sfs[22] = new StructField("lval_ag", new DecimalType(12, 0), true, null);
        sfs[23] = new StructField("rval_ag", new DecimalType(12, 0), true, null);
        sfs[24] = new StructField("lval_rg", new DecimalType(12, 0), true, null);
        sfs[25] = new StructField("rval_rg", new DecimalType(12, 0), true, null);
        sfs[26] = new StructField("lval_ar", new DecimalType(12, 0), true, null);
        sfs[27] = new StructField("rval_ar", new DecimalType(12, 0), true, null);
        sfs[28] = new StructField("lval_bl", new DecimalType(12, 0), true, null);
        sfs[29] = new StructField("rval_bl", new DecimalType(12, 0), true, null);
        sfs[30] = new StructField("dk_tano", new StringType(), true, null);
        sfs[31] = new StructField("cserialno", new StringType(), true, null);
        sfs[32] = new StructField("sk_tradeacco_reg", new DecimalType(12, 0), true, null);
        sfs[33] = new StructField("agencyno", new StringType(), true, null);
        sfs[34] = new StructField("netno", new StringType(), true, null);
        sfs[35] = new StructField("dk_share_type", new StringType(), true, null);
        sfs[36] = new StructField("sk_agency_of_lv1", new DecimalType(12, 0), true, null);
        sfs[37] = new StructField("sk_region_of_lv1", new DecimalType(12, 0), true, null);
        sfs[38] = new StructField("dk_arsplt_chg_flag", new StringType(), true, null);
        sfs[39] = new StructField("dk_anal_dymic", new StringType(), true, null);


        StructType st = new StructType(sfs);
        r[0] = SparkSplitUtils.gr(RowFactory.create(
                //dk_tano.cserialno,sk_account,sk_invpty,dk_cust_type,sk_product,sk_agency_of_lv1,sk_region_of_lv1
                "*.*,-1,-1,*,-1,-1,-1",                //0 key
                new BigDecimal(5854), //1 area_rule_id
                new BigDecimal(116),//2 sk_product_type
                new BigDecimal(-1),//3 sk_product
                "*",               //4 dk_cust_type
                new BigDecimal(-1),//5 sk_invpty_type
                new BigDecimal(-1),//6 sk_invpty_of_cust
                new BigDecimal(-1),//7 sk_account_type
                new BigDecimal(-1),//8 sk_account_of_fd
                new BigDecimal(-1),//9 sk_agency
                "*",               //10 dk_agency_type
                new BigDecimal(6),//11 sk_region
                "01",               //12 dk_org_tree_of_branch
                new BigDecimal(3734),//13 sk_invpty_of_org_brnch
                "01",               //14 dk_org_tree_of_bl
                new BigDecimal(3734),//15 sk_invpty_of_org_bl
                "2",               //16 dk_custmngr_type
                new BigDecimal(2865473),//17 sk_invpty_of_custmngr
                new BigDecimal(7020),//18 priority
                new BigDecimal(0.5),//19 ratio
                new BigDecimal(19000101),  //20 effective_from
                new BigDecimal(20180101),//21 effective_to
                new BigDecimal(-1),//22 lval_ag
                new BigDecimal(999999999999L),//23 rval_ag
                new BigDecimal(-1),//24 lval_rg
                new BigDecimal(999999999999L),//25 rval_rg
                new BigDecimal(-1),//26 lval_ar
                new BigDecimal(999999999999L),//27 rval_ar
                new BigDecimal(-1),//28 lval_bl
                new BigDecimal(999999999999L),//29 rval_bl
                "*",//30 dk_tano
                "*",//31 cserialno
                new BigDecimal(-1),//32 sk_tradeacco_reg
                "*",//33 agencyno
                "*",//34 netno
                "*",//35 dk_share_type
                new BigDecimal(-1),//36 sk_agency_of_lv1
                new BigDecimal(-1),//37 sk_region_of_lv1
                "1",//38 dk_arsplt_chg_flag
                "N"//39 dk_anal_dymic
        ), st);

        r[1] = SparkSplitUtils.gr(RowFactory.create(
                //dk_tano.cserialno,sk_account,sk_invpty,dk_cust_type,sk_product,sk_agency_of_lv1,sk_region_of_lv1
                "*.*,-1,-1,*,-1,-1,-1",                //0 key
                new BigDecimal(6025), //1 area_rule_id
                new BigDecimal(116),//2 sk_product_type
                new BigDecimal(-1),//3 sk_product
                "*",               //4 dk_cust_type
                new BigDecimal(-1),//5 sk_invpty_type
                new BigDecimal(-1),//6 sk_invpty_of_cust
                new BigDecimal(-1),//7 sk_account_type
                new BigDecimal(-1),//8 sk_account_of_fd
                new BigDecimal(-1),//9 sk_agency
                "*",               //10 dk_agency_type
                new BigDecimal(6),//11 sk_region
                "01",               //12 dk_org_tree_of_branch
                new BigDecimal(3734),//13 sk_invpty_of_org_brnch
                "01",               //14 dk_org_tree_of_bl
                new BigDecimal(3734),//15 sk_invpty_of_org_bl
                "2",               //16 dk_custmngr_type
                new BigDecimal(14290256),//17 sk_invpty_of_custmngr
                new BigDecimal(7020),//18 priority
                new BigDecimal(0.5),//19 ratio
                new BigDecimal(19000101),  //20 effective_from
                new BigDecimal(20180101),//21 effective_to
                new BigDecimal(-1),//22 lval_ag
                new BigDecimal(999999999999L),//23 rval_ag
                new BigDecimal(-1),//24 lval_rg
                new BigDecimal(999999999999L),//25 rval_rg
                new BigDecimal(-1),//26 lval_ar
                new BigDecimal(999999999999L),//27 rval_ar
                new BigDecimal(-1),//28 lval_bl
                new BigDecimal(999999999999L),//29 rval_bl
                "*",//30 dk_tano
                "*",//31 cserialno
                new BigDecimal(-1),//32 sk_tradeacco_reg
                "*",//33 agencyno
                "*",//34 netno
                "*",//35 dk_share_type
                new BigDecimal(-1),//36 sk_agency_of_lv1
                new BigDecimal(-1),//37 sk_region_of_lv1
                "1",//38 dk_arsplt_chg_flag
                "N"//39 dk_anal_dymic
        ), st);

        r[2] = SparkSplitUtils.gr(RowFactory.create(
                //dk_tano.cserialno,sk_account,sk_invpty,dk_cust_type,sk_product,sk_agency_of_lv1,sk_region_of_lv1
                "*.*,-1,-1,*,-1,-1,-1",                //0 key
                new BigDecimal(6061), //1 area_rule_id
                new BigDecimal(-1),//2 sk_product_type
                new BigDecimal(-1),//3 sk_product
                "*",               //4 dk_cust_type
                new BigDecimal(-1),//5 sk_invpty_type
                new BigDecimal(-1),//6 sk_invpty_of_cust
                new BigDecimal(-1),//7 sk_account_type
                new BigDecimal(-1),//8 sk_account_of_fd
                new BigDecimal(-1),//9 sk_agency
                "*",               //10 dk_agency_type
                new BigDecimal(-1),//11 sk_region
                "*",               //12 dk_org_tree_of_branch
                new BigDecimal(4117),//13 sk_invpty_of_org_brnch
                "*",               //14 dk_org_tree_of_bl
                new BigDecimal(-1),//15 sk_invpty_of_org_bl
                "*",               //16 dk_custmngr_type
                new BigDecimal(-1),//17 sk_invpty_of_custmngr
                new BigDecimal(8000),//18 priority
                new BigDecimal(1),//19 ratio
                new BigDecimal(19000101),  //20 effective_from
                new BigDecimal(99991231),//21 effective_to
                new BigDecimal(-1),//22 lval_ag
                new BigDecimal(999999999999L),//23 rval_ag
                new BigDecimal(-1),//24 lval_rg
                new BigDecimal(999999999999L),//25 rval_rg
                new BigDecimal(-1),//26 lval_ar
                new BigDecimal(999999999999L),//27 rval_ar
                new BigDecimal(-1),//28 lval_bl
                new BigDecimal(999999999999L),//29 rval_bl
                "*",//30 dk_tano
                "*",//31 cserialno
                new BigDecimal(-1),//32 sk_tradeacco_reg
                "*",//33 agencyno
                "*",//34 netno
                "*",//35 dk_share_type
                new BigDecimal(-1),//36 sk_agency_of_lv1
                new BigDecimal(-1),//37 sk_region_of_lv1
                "1",//38 dk_arsplt_chg_flag
                "N"//39 dk_anal_dymic
        ), st);
    }

    static Row[] initData() {
        Row[] r = new Row[1];
        StructField[] sfs = new StructField[26];
        sfs[0] = new StructField("trd_st_trx_serialno", new DecimalType(12, 0), true, null);
        sfs[1] = new StructField("sk_invpty_type", new DecimalType(12, 0), true, null);
        sfs[2] = new StructField("sk_invpty", new DecimalType(12, 0), true, null);
        sfs[3] = new StructField("sk_account_type", new DecimalType(12, 0), true, null);
        sfs[4] = new StructField("sk_account", new DecimalType(12, 0), true, null);

        sfs[5] = new StructField("sk_product_type", new DecimalType(12, 0), true, null);
        sfs[6] = new StructField("sk_product", new DecimalType(12, 0), true, null);
        sfs[7] = new StructField("dk_agency_type", new StringType(), true, null);
        sfs[8] = new StructField("sk_agency", new DecimalType(12, 0), true, null);
        sfs[9] = new StructField("sk_region", new DecimalType(12, 0), true, null);
        sfs[10] = new StructField("dk_tano", new StringType(), true, null);

        sfs[11] = new StructField("lval_ag", new DecimalType(12, 0), true, null);
        sfs[12] = new StructField("rval_ag", new DecimalType(12, 0), true, null);
        sfs[13] = new StructField("lval_rg", new DecimalType(12, 0), true, null);
        sfs[14] = new StructField("rval_rg", new DecimalType(12, 0), true, null);
        sfs[15] = new StructField("lval_ar", new DecimalType(12, 0), true, null);
        sfs[16] = new StructField("rval_ar", new DecimalType(12, 0), true, null);

        sfs[17] = new StructField("sk_date", new DecimalType(8, 0), true, null);
        sfs[18] = new StructField("sk_agency_of_lv1", new DecimalType(12, 0), true, null);
        sfs[19] = new StructField("sk_region_of_lv1", new DecimalType(12, 0), true, null);
        sfs[20] = new StructField("cserialno", new StringType(), true, null);
        sfs[21] = new StructField("dk_cust_type", new StringType(), true, null);
        sfs[22] = new StructField("sk_tradeacco_reg", new DecimalType(12, 0), true, null);
        sfs[23] = new StructField("agencyno", new StringType(), true, null);
        sfs[24] = new StructField("netno", new StringType(), true, null);
        sfs[25] = new StructField("dk_share_type", new StringType(), true, null);

        StructType st = new StructType(sfs);


        r[0] = SparkSplitUtils.gr(RowFactory.create(
                new BigDecimal(753862371L),  //0 trd_st_trx_serialno
                new BigDecimal(-1), //1 sk_invpty_type
                new BigDecimal(1662046),//2 sk_invpty
                new BigDecimal(-1),//3 sk_account_type
                new BigDecimal(3871558),//4 sk_account
                new BigDecimal(-1),//5 sk_product_type
                new BigDecimal(50),//6 sk_product
                "BNK",               //7 dk_agency_type
                new BigDecimal(830411),//8 sk_agency
                new BigDecimal(6),//9 sk_region
                "18",               //10 dk_tano
                new BigDecimal(158495),//11 lval_ag
                new BigDecimal(159050), //12 rval_ag
                new BigDecimal(830),//13 lval_rg
                new BigDecimal(867),//14 rval_rg
                new BigDecimal(0),//15 lval_ar
                new BigDecimal(0), //16 rval_ar
                new BigDecimal(20140107),//17 sk_date
                new BigDecimal(806311),//18 sk_agency_of_lv1
                new BigDecimal(6),//19 sk_region_of_lv1
                "0040008586773",  //20 cserialno
                "1",//21 dk_cust_type
                new BigDecimal(8499696),//22 sk_tradeacco_reg
                "004",//23 agencyno
                "004",//24 netno
                "A"//25 dk_share_type
        ), st);
		/*
		r[1]=SparkSplitUtils.gr(RowFactory.create( 				
				new BigDecimal(753862371L),  //0 trd_st_trx_serialno
				new BigDecimal(-1), //1 sk_invpty_type
				new BigDecimal(-1),//2 sk_invpty
				new BigDecimal(-1),//3 sk_account_type
				new BigDecimal(2),//4 sk_account				
				new BigDecimal(-1),//5 sk_product_type
				new BigDecimal(-1),//6 sk_product
				"*",			   //7 dk_agency_type
				new BigDecimal(-1),//8 sk_agency
				new BigDecimal(-1),//9 sk_region
				"*",			   //10 dk_tano
				new BigDecimal(10),//11 lval_ag
				new BigDecimal(100), //12 rval_ag
				new BigDecimal(10),//13 lval_rg
				new BigDecimal(100),//14 rval_rg
				new BigDecimal(0),//15 lval_ar 
				new BigDecimal(0), //16 rval_ar
				
				new BigDecimal(20200520),//17 sk_date
				new BigDecimal(-1),//18 sk_agency_of_lv1
				new BigDecimal(-1),//19 sk_region_of_lv1
				"*",  //20 cserialno
				"*",//21 dk_cust_type
				new BigDecimal(-1),//22 sk_tradeacco_reg
				"002",//23 agencyno
				"002",//24 netno
				"A"//25 dk_share_type
				),st); 
				*/
        return r;
    }
}
