package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import cn.com.businessmatrix.utils.SparkSplitUtils;

public class YHSparkAreaSplit {

    /*
     * 管理区划拆分
     */

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hc = new HiveContext(sc);

        String pi_src_table = args[0];
        String pi_tgt_table = args[1];
        String schema = args[2];
        //String pi_startdate = args[4];
        //String pi_enddate = args[5];
        //String pi_tano = args[6];

        String sql_schema =
                "SELECT t.trd_st_trx_serialno,\n" +
                        "       t.area_rule_id,\n" +
                        "       t.dk_org_tree,\n" +
                        "       t.sk_org,\n" +
                        "       t.dk_org_tree_of_bl,\n" +
                        "       t.sk_org_of_bl,\n" +
                        "       t.dk_custmngr_type,\n" +
                        "       t.sk_invpty_of_custmngr,\n" +
                        "       t.ratio,\n" +
                        "       t.sk_date,\n" +
                        //"       t.effective_to,\n" +
                        "       cast(1 AS decimal(12, 0)) AS lval_ar,\n" +
                        "       cast(1 AS decimal(12, 0)) AS rval_ar,\n" +
                        "       cast(1 AS decimal(12, 0)) AS lval_bl,\n" +
                        "       cast(1 AS decimal(12, 0)) AS rval_bl\n" +
                        "  FROM " + schema + ".trd_st_assoc_area t\n" +
                        " WHERE 1 = 2";

        System.out.println("================sql_schema====================");
        System.out.println(sql_schema);
        System.out.println("=================sql_schema===================");
        StructType st = hc.sql(sql_schema).schema();

        String sql_data = "SELECT t.trx_serialno,\n" +
                "        t.sk_invpty_type,\n" +
                "       t.sk_invpty,\n" +
                "       t.sk_account_type,\n" +
                "       t.sk_account,\n" +
                "       t.sk_prod_type as sk_product_type,\n" +
                "       t.sk_product,\n" +
                "       t.dk_agency_type,\n" +
                "       t.sk_agency,\n" +
                "       t.sk_region,\n" +
                "       t.dk_tano,\n" +
                "       t.lval_ag,\n" +
                "       t.rval_ag,\n" +
                "       t.lval_rg,\n" +
                "       t.rval_rg,\n" +
                "       t.lval_ar,\n" +
                "       t.rval_ar,\n" +
                "       t.sk_date,\n" +
                "       t.sk_agency_of_lv1,\n" +
                "       t.sk_region_of_lv1,\n" +
                "       t.cserialno,\n" +
                "       t.dk_cust_type,\n" +
                "       t.sk_tradeacco_reg,\n" +
                "       t.agencyno,\n" +
                "       t.netno,\n" +
                "       t.dk_share_type,\n" +
                "       t.dk_bourseflag \n" +
                "  FROM  " + schema + "." + pi_src_table + " t\n" +
                " ORDER BY t.sk_account,\n" +
                "          t.sk_invpty,\n" +
                "          t.sk_region_of_lv1,\n" +
                "          t.sk_agency_of_lv1";


        System.out.println("================sql_data====================");
        System.out.println(sql_data);
        System.out.println("=================sql_data===================");
        Dataset<Row> ipdf = hc.sql(sql_data);

        String sql_rule =
                "WITH a AS\n" +
                        "  (SELECT DISTINCT nvl(t1.dk_tano,'*') AS dk_tano,\n" +
                        "                   nvl(t1.cserialno,'*') AS cserialno,\n" +
                        "                   nvl(t1.sk_account,cast(-1 as decimal(12,0))) AS sk_account,\n" +
                        "                   nvl(t1.sk_invpty,cast(-1 as decimal(12,0))) AS sk_invpty,\n" +
                        "                   nvl(t1.sk_product,cast(-1 as decimal(12,0))) AS sk_product,\n" +
                        "                   nvl(t2.sk_agency_of_lv1,cast(-1 as decimal(12,0))) AS sk_agency_of_lv1,\n" +
                        "                   nvl(t3.sk_region_of_lv1,cast(-1 as decimal(12,0))) AS sk_region_of_lv1,\n" +
                        "                   nvl(t1.dk_cust_type,'*') AS dk_cust_type\n" +
                        "   FROM " + schema + ".org_areasplit_common t1\n" +
                        "   LEFT JOIN " + schema + ".ip_agency_hierarchy t2 ON t1.sk_agency = t2.sk_agency\n" +
                        "   AND t2.dk_agency_tree = '01'\n" +
                        "   LEFT JOIN " + schema + ".comm_region_hierarchy t3 ON t1.sk_region = t3.sk_region\n" +
                        "   AND t3.dk_region_tree = '01')\n" +
                        "SELECT concat_ws(',',concat_ws('.',dk_tano,cserialno), cast(sk_account AS string),cast(sk_invpty AS string),dk_cust_type,cast(sk_product AS string),cast(sk_agency_of_lv1 AS string),cast(sk_region_of_lv1 AS string)) AS KEY,\n" +
                        "       dk_tano,\n" +
                        "       cserialno,\n" +
                        "       sk_account,\n" +
                        "       sk_invpty,\n" +
                        "       dk_cust_type,\n" +
                        "       sk_product,\n" +
                        "       sk_agency_of_lv1,\n" +
                        "       sk_region_of_lv1\n" +
                        "FROM a";
        System.out.println("================sql_rule====================");
        System.out.println(sql_rule);
        System.out.println("=================sql_rule===================");

        //DataFrame rdf = hc.sql(sql_rule);

        String sql_mapp = "WITH a AS\n" +
                "  (SELECT t.area_rule_id,\n" +
                "          nvl(t.sk_product_type,   cast(-1 as decimal(12,0))) AS sk_product_type,\n" +
                "          nvl(t.sk_product,        cast(-1 as decimal(12,0))) AS sk_product,\n" +
                "          nvl(t.dk_cust_type,'*')                             AS dk_cust_type,\n" +
                "          nvl(t.sk_invpty_type,    cast(-1 as decimal(12,0))) AS sk_invpty_type,\n" +
                "          nvl(t.sk_invpty,         cast(-1 as decimal(12,0))) AS sk_invpty,\n" +
                "          nvl(t.sk_account_type,   cast(-1 as decimal(12,0))) AS sk_account_type,\n" +
                "          nvl(t.sk_account,        cast(-1 as decimal(12,0))) AS sk_account,\n" +
                "          nvl(t.sk_agency,         cast(-1 as decimal(12,0))) AS sk_agency,\n" +
                "          nvl(t.dk_agency_type, '*')                          AS dk_agency_type,\n" +
                "          nvl(t.sk_region,         cast(-1 as decimal(12,0))) AS sk_region,\n" +
                "          t.dk_org_tree                                       AS dk_org_tree,\n" +
                "          t.sk_org                                            AS sk_org,\n" +
                "          t.dk_org_tree_of_bl,\n" +
                "          t.sk_org_of_bl                                      AS sk_org_of_bl,\n" +
                "          t.dk_custmngr_type,\n" +
                "          t.sk_invpty_of_custmngr,\n" +
                "          t.priority,\n" +
                "          t.ratio                                             AS ratio,\n" +
                "          greatest(t.effective_from, nvl(t2.effective_from, cast(0        as decimal(8,0))), nvl(t3.effective_from, cast(0        as decimal(8,0))), nvl(t4.effective_from,cast(0        as decimal(8,0))), nvl(t5.effective_from,cast(0        as decimal(8,0)))) AS effective_from,\n" +
                "          least(t.effective_to,      nvl(t2.effective_to,   cast(99991231 as decimal(8,0))), nvl(t3.effective_to,   cast(99991231 as decimal(8,0))), nvl(t4.effective_to,  cast(99991231 as decimal(8,0))), nvl(t5.effective_to,  cast(99991231 as decimal(8,0)))) AS effective_to,\n" +
                "          nvl(t2.lval, cast(-1 as decimal(12,0))) AS lval_ag,\n" +
                "          nvl(t2.rval, cast(999999999999 as decimal(12,0))) AS rval_ag,\n" +
                "          nvl(t3.lval, cast(-1 as decimal(12,0))) AS lval_rg,\n" +
                "          nvl(t3.rval, cast(999999999999 as decimal(12,0))) AS rval_rg,\n" +
                "          nvl(t5.lval, cast(-1 as decimal(12,0))) AS lval_ar,\n" +
                "          nvl(t5.rval, cast(999999999999 as decimal(12,0))) AS rval_ar,\n" +
                "          nvl(t4.lval, cast(-1 as decimal(12,0))) AS lval_bl,\n" +
                "          nvl(t4.rval, cast(999999999999 as decimal(12,0))) AS rval_bl,\n" +
                "          nvl(t.dk_tano,'*') AS dk_tano,\n" +
                "          nvl(t.cserialno,'*') AS cserialno,\n" +
                "          nvl(t7.sk_tradeacco_reg,cast(-1 as decimal(12,0))) AS sk_tradeacco_reg,\n" +
                "          nvl(t.agencyno,'*') AS agencyno,\n" +
                "          nvl(t.netno,'*') AS netno,\n" +
                "          nvl(t.dk_share_type,'*') AS dk_share_type,\n" +
                "          t2.sk_agency_of_lv1 AS sk_agency_of_lv1,\n" +
                "          t3.sk_region_of_lv1 AS sk_region_of_lv1,\n" +
                "          nvl(t.dk_arsplt_chg_flag,'0') AS dk_arsplt_chg_flag,\n" +
                "          nvl(t.dk_bourseflag, '*') AS dk_bourseflag \n" +
                "   FROM " + schema + ".org_areasplit_common t\n" +
                "   LEFT JOIN " + schema + ".ip_agency_hierarchy t2 ON t.sk_agency = t2.sk_agency\n" +
                "   AND t2.dk_agency_tree = '01'\n" +
                "   LEFT JOIN " + schema + ".comm_region_hierarchy t3 ON t.sk_region = t3.sk_region\n" +
                "   AND t3.dk_region_tree = '01'\n" +
                "   LEFT JOIN " + schema + ".org_branch_hierarchy t4 ON t.sk_org_of_bl = t4.sk_org\n" +
                "   AND t.dk_org_tree_of_bl=t4.dk_org_tree\n" +
                "   LEFT JOIN " + schema + ".org_branch_hierarchy t5 ON t.sk_org = t5.sk_org\n" +
                "   AND t.dk_org_tree=t5.dk_org_tree\n" +
                "   LEFT JOIN " + schema + ".agrm_tradeaccount_ds t6 ON t.sk_account_of_ag =t6.sk_account\n" +
                "   LEFT JOIN " + schema + ".agrm_tradeacco_reg t7 ON t6.bk_tradeaccount =t7.bk_tradeaccount\n" +
                "   AND t6.agencyno=t7.agencyno"
                + " )\n" +
                " select concat_ws(',',concat_ws('.',dk_tano,cserialno), cast(sk_account AS string),cast(sk_invpty AS string),dk_cust_type,cast(sk_product AS string),cast(sk_agency_of_lv1 AS string),cast(sk_region_of_lv1 AS string)) AS KEY, \n" +
                "       t.area_rule_id,\n" +
                "       t.sk_product_type,\n" +
                "       t.sk_product,\n" +
                "       t.dk_cust_type,\n" +
                "       t.sk_invpty_type,\n" +
                "       t.sk_invpty,\n" +
                "       t.sk_account_type,\n" +
                "       t.sk_account,\n" +
                "       t.sk_agency,\n" +
                "       t.dk_agency_type,\n" +
                "       t.sk_region,\n" +
                "       t.dk_org_tree,\n" +
                "       t.sk_org,\n" +
                "       t.dk_org_tree_of_bl,\n" +
                "       t.sk_org_of_bl,\n" +
                "       t.dk_custmngr_type,\n" +
                "       t.sk_invpty_of_custmngr,\n" +
                "       t.priority,\n" +
                "       t.ratio,\n" +
                "       t.effective_from,\n" +
                "       t.effective_to,\n" +
                "       t.lval_ag,\n" +
                "       t.rval_ag,\n" +
                "       t.lval_rg,\n" +
                "       t.rval_rg,\n" +
                "       t.lval_ar,\n" +
                "       t.rval_ar,\n" +
                "       t.lval_bl,\n" +
                "       t.rval_bl,\n" +
                "       t.dk_tano,\n" +
                "       t.cserialno,\n" +
                "       t.sk_tradeacco_reg,\n" +
                "       t.agencyno,\n" +
                "       t.netno,\n" +
                "       t.dk_share_type,\n" +
                "       t.dk_arsplt_chg_flag,\n" +
                "       t.dk_bourseflag,\n" +
                "       t.sk_agency_of_lv1,\n" +
                "       t.sk_region_of_lv1\n" +
                "FROM a t\n" +
                "ORDER BY t.priority,\n" +
                "         t.sk_product DESC,\n" +
                "         t.sk_agency_of_lv1 DESC,\n" +
                "         t.sk_region_of_lv1 DESC";

        System.out.println("================sql_mapp====================");
        System.out.println(sql_mapp);
        System.out.println("=================sql_mapp===================");
        Dataset<Row> ddf = hc.sql(sql_mapp);

        //Row[] rules = rdf.collect();
        Row[] rs = (Row[]) ddf.collect();
        Map<String, List<Row>> map = new HashMap<String, List<Row>>();
        List<Row> ls = null;
        String key = null;
        for (int i = 0; i < rs.length; i++) {
            key = rs[i].getString(0);
            ls = map.get(key);
            if (ls == null)
                ls = new ArrayList<Row>();
            ls.add(rs[i]);
            map.put(key, ls);
        }
        //System.out.println("=================start row handle===================");

        JavaRDD<List<Row>> w = ipdf.toJavaRDD().map(new Function<Row, List<Row>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public List<Row> call(Row r) throws Exception {

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
                String pi_dk_bourseflag = (String) r.getAs("dk_bourseflag");


                BigDecimal pi_sk_prod_type = (BigDecimal) r.getAs("sk_product_type");
                BigDecimal pi_sk_product = (BigDecimal) r.getAs("sk_product");
                BigDecimal pi_sk_invpty_type = null;//(BigDecimal)r.getAs("sk_invpty_type");
                BigDecimal pi_sk_invpty = (BigDecimal) r.getAs("sk_invpty");
                BigDecimal pi_sk_account_type = null;//(BigDecimal)r.getAs("sk_account_type");
                BigDecimal pi_sk_account = (BigDecimal) r.getAs("sk_account");
                BigDecimal pi_sk_tradeacco_reg = (BigDecimal) r.getAs("sk_tradeacco_reg");
                BigDecimal pi_sk_date = (BigDecimal) r.getAs("sk_date");

                BigDecimal pi_lval_ag = (BigDecimal) r.getAs("lval_ag");
                BigDecimal pi_rval_ag = (BigDecimal) r.getAs("rval_ag");
                BigDecimal pi_lval_rg = (BigDecimal) r.getAs("lval_rg");
                BigDecimal pi_rval_rg = (BigDecimal) r.getAs("rval_rg");
                BigDecimal pi_sk_agency_of_lv1 = (BigDecimal) r.getAs("sk_agency_of_lv1");
                BigDecimal pi_sk_region_of_lv1 = (BigDecimal) r.getAs("sk_region_of_lv1");

                v_count0 = SparkSplitUtils.getCateCode(pi_cserialno);
                v_count1 = SparkSplitUtils.getCateCode(pi_sk_account);
                v_count2 = SparkSplitUtils.getCateCode(pi_sk_invpty);
                v_count3 = SparkSplitUtils.getCateCode(pi_dk_cust_type);
                v_count4 = SparkSplitUtils.getCateCode(pi_sk_product);
                v_count5 = SparkSplitUtils.getCateCode(pi_sk_agency_of_lv1);
                v_count6 = SparkSplitUtils.getCateCode(pi_sk_region_of_lv1);

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
                                            innerMap.put(vc_key, map.get(vc_key));
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
                //double v_lv_remain_ratio=0.0d;
                double v_accu_ratio_last = 0.0d;
                double v_e = 0.0000001d;
                BigDecimal v_priority_last = null;
                //vc_pre_rule_id =BD_UNKNOWN;
                Iterator<Row> it = orMap.iterator();

                Row rule = null;
                BigDecimal priority = null;
                BigDecimal area_rule_id = null;
                //String dk_anal_dymic=null;
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
                                        && SparkSplitUtils.matchRule(pi_dk_bourseflag, (String) rule.getAs("dk_bourseflag"))

                        ) {
                            priority = (BigDecimal) rule.getAs("priority");
                            area_rule_id = (BigDecimal) rule.getAs("area_rule_id");

                            //dk_anal_dymic=(String)rule.getAs("dk_anal_dymic");
                            ratio = (BigDecimal) rule.getAs("ratio");

                            if (
                                    SparkSplitUtils.nvl(v_priority_last, new BigDecimal(-4242)).compareTo(SparkSplitUtils.nvl(priority)) != 0
                            ) {
                                v_accu_ratio_last = v_accu_ratio;
                            }

                            v_ratio = v_accu_ratio_last * ratio.doubleValue();
                            v_priority_last = priority;

                            ret.add(RowFactory.create(
                                    (BigDecimal) r.getAs(0),//流水号，唯一值
                                    area_rule_id,
                                    (String) rule.getAs("dk_org_tree"),
                                    (BigDecimal) rule.getAs("sk_org"),
                                    (String) rule.getAs("dk_org_tree_of_bl"),
                                    (BigDecimal) rule.getAs("sk_org_of_bl"),
                                    (String) rule.getAs("dk_custmngr_type"),
                                    (BigDecimal) rule.getAs("sk_invpty_of_custmngr"),
                                    new BigDecimal(v_ratio),
                                    pi_sk_date,
                                    (BigDecimal) rule.getAs("lval_ar"),
                                    (BigDecimal) rule.getAs("rval_ar"),
                                    (BigDecimal) rule.getAs("lval_bl"),
                                    (BigDecimal) rule.getAs("rval_bl")
                            ));
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
                            null,//area_rule_id
                            null,//dk_org_tree
                            new BigDecimal(-1),//sk_org
                            null,//dk_org_tree_of_bl
                            new BigDecimal(-1),//sk_org_of_bl
                            null,//dk_custmngr_type
                            new BigDecimal(-1),//sk_invpty_of_custmngr
                            new BigDecimal(v_accu_ratio), //ratio
                            pi_sk_date,//effective_from
                            new BigDecimal(-1),//lval_ar
                            new BigDecimal(999999999999L),//12g9,rval_ar
                            new BigDecimal(-1),//lval_bl
                            new BigDecimal(999999999999L) //rval_bl
                    ));
                }
                //System.out.println("=============return========="+ret);
                return ret;
            }
        });

        JavaRDD<Row> x = w.flatMap(new FlatMapFunction<List<Row>, Row>() {
            private static final long serialVersionUID = -6111647437563220114L;

            @Override
            public Iterator<Row> call(List<Row> arg0) throws Exception {
                return arg0.iterator();
            }
        });

        //hc.sql("truncate table "+schema+"."+pi_table);
        hc.applySchema(x, st).write().mode(SaveMode.Overwrite).saveAsTable(schema + "." + pi_tgt_table);
        //String[] s=pi_table.split("/");	
        //System.out.println(s[0]+s[s.length-2]);
        //hc.applySchema(x, st).write().mode(SaveMode.Overwrite).saveAsTable(s[0]+s[s.length-2]);
        //hc.refreshTable(s[0]+s[s.length-2]);
        sc.close();
    }
}

