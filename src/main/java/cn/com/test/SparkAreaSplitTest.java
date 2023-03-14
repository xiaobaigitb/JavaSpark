package cn.com.test;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import cn.com.businessmatrix.utils.SparkSplitUtils;

public class SparkAreaSplitTest {

    /*
     * 管理区划拆分-非拉链场景
     */

    public static void main(String[] args) {

        String savePath = "D:\\spark_dev\\SPARK_DEMO\\resources\\data\\parquet\\trd_ta_saletran_arsplt";
        //便利删除文件
        /*boolean b = deleteDir(new File(savePath));
        if (!b){
            return;
        }*/

        System.setProperty("hadoop.home.dir", "D:\\spark_dev\\hadoop-common-2.2.0-bin-master");

        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .appName("SparkAreaSplitTest")
                .master("local[1]")
                .getOrCreate();

        List<StructField> lookdf_st_list = new ArrayList<StructField>();
        lookdf_st_list.add(DataTypes.createStructField("trd_st_trx_serialno", DataTypes.createDecimalType(12, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("dk_tano", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("ta_cfm_date", DataTypes.createDecimalType(8, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("dk_cust_type", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("area_rule_id", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("dk_org_tree_of_branch", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_org_brnch", DataTypes.createDecimalType(12, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("dk_org_tree_of_bl", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_org_bl", DataTypes.createDecimalType(12, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("dk_custmngr_type", DataTypes.StringType, true));
        lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_custmngr", DataTypes.createDecimalType(12, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("sk_product", DataTypes.createDecimalType(12, 0), true));
        lookdf_st_list.add(DataTypes.createStructField("div_ratio", DataTypes.createDecimalType(5, 2), true));
        lookdf_st_list.add(DataTypes.createStructField("dk_system_of_sdata", DataTypes.StringType, true));

/*
		lookdf_st_list.add(DataTypes.createStructField("trd_st_trx_serialno",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_tano",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("ta_cfm_date",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_cust_type",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("area_rule_id",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_org_tree_of_branch",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_org_brnch",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_org_tree_of_bl",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_org_bl",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_custmngr_type",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("sk_invpty_of_custmngr",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("sk_product",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("div_ratio",DataTypes.StringType,true));
		lookdf_st_list.add(DataTypes.createStructField("dk_system_of_sdata",DataTypes.StringType,true));
*/

        System.out.println("================sql_schema====================");
        lookdf_st_list.parallelStream();

        System.out.println(lookdf_st_list);

        System.out.println("=================sql_schema===================");
        //StructType st = dfRead.option("dbtable", sql_schema).load().schema();
        Dataset<Row> ipdf = hc.read().parquet("D:\\spark_dev\\SPARK_DEMO\\resources\\data\\parquet\\trd_ta_saletran");
        System.out.println("=================sql_data===================");
        ipdf.printSchema();
        ipdf.show();
        System.out.println("=================sql_data===================");
        //Dataset<Row> ipdf = dfRead.option("dbtable", sql_data).load();

        Dataset<Row> ddf = hc.read().parquet("D:\\spark_dev\\SPARK_DEMO\\resources\\data\\parquet\\org_areasplit_common");
        System.out.println("================sql_mapp====================");
        ddf.printSchema();
        ddf.show();
        System.out.println("================sql_mapp====================");
        //Dataset<Row> ddf = dfRead.option("dbtable", sql_mapp).load();

        //Row[] rules = rdf.collect();
        Row[] rs = (Row[]) ddf.orderBy("priority").collect();
        Map<String, List<Row>> ruleMap = new HashMap<String, List<Row>>();
        List<Row> ls = null;
        String key = null;
        for (int i = 0; i < rs.length; i++) {
            key = rs[i].getString(0);
            ls = ruleMap.get(key);
            if (ls == null)
                ls = new ArrayList<Row>();
            ls.add(rs[i]);
            ruleMap.put(key, ls);
        }

        System.out.println("=================start row handle===================");

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


                String vc_pre_rule_id = null;
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
                String pi_dk_system_of_sdata = (String) r.getAs("dk_system_of_sdata");


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
                                            innerMap.put(vc_key, ruleMap.get(vc_key));


                                        }
                                    }
                                }
                            }
                        }
                    }

                }// end for


                System.out.println("===========================innerMap===================================");
                for (String innerkey : innerMap.keySet()
                ) {
                    System.out.println("innerKey: " + innerkey + "  innerValue: " + innerMap.get(innerkey));
                }

                System.out.println("===========================innerMap===================================");

                List<Row> orMap = SparkSplitUtils.reorder(innerMap);


                System.out.println("===========================orMap===================================");
                for (int i = 0; i <= orMap.size() - 1; i++) {
                    System.out.println("key：" + i + " value: " + orMap.get(i));
                }
                System.out.println("============================orMap==================================");

                double v_accu_ratio = 1.0d;
                double v_ratio = 0.0d;
                double v_lv_remain_ratio = 0.0d;
                double v_e = 0.0000001d;
                vc_pre_rule_id = SparkSplitUtils.S_UNKNOWN;
                Iterator<Row> it = orMap.iterator();

                Row rule = null;
                BigDecimal priority = null;
                String area_rule_id = null;
                String dk_anal_dymic = null;
                BigDecimal ratio = null;

                while (it.hasNext()) {
                    System.out.println("=============data=========" + it);

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
                            area_rule_id = (String) rule.getAs("area_rule_id");
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
                                    pi_tano,
                                    pi_sk_date,
                                    pi_dk_cust_type,
                                    area_rule_id,
                                    rule.getAs("dk_org_tree"),
                                    rule.getAs("sk_org"),
                                    rule.getAs("dk_org_tree_of_bl"),
                                    rule.getAs("sk_org_of_bl"),
                                    rule.getAs("dk_custmngr_type"),
                                    rule.getAs("sk_invpty_of_custmngr"),
                                    pi_sk_product,
                                    new BigDecimal(v_ratio),
                                    pi_dk_system_of_sdata
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
                            pi_tano,
                            pi_sk_date,
                            pi_dk_cust_type,
                            null,
                            null,
                            new BigDecimal(-1),
                            null,
                            new BigDecimal(-1),
                            null,
                            new BigDecimal(-1),
                            pi_sk_product,
                            new BigDecimal(v_accu_ratio),
                            pi_dk_system_of_sdata
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


        StructType lookdf_st = DataTypes.createStructType(lookdf_st_list);
        hc.createDataFrame(x, lookdf_st)
                //.repartition(4)   设置并行度，会写入多个文件中。
                .write()
                .format("parquet")
                .mode(SaveMode.Append).save(savePath);

        System.out.println("====================END createDataFrame=======================");
        Dataset<Row> lookdf = hc.read().parquet(savePath);
        lookdf.printSchema();
        lookdf.show();
        hc.close();
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}

