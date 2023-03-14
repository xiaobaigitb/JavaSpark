package cn.com.businessmatrix.other;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.com.businessmatrix.domain.DateRange;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import cn.com.businessmatrix.utils.ImpalaDialect;
import cn.com.businessmatrix.utils.SparkSplitUtils;

/*修改记录：
 * 20220927 Luogong
 * 支持半自动重载，增加pi_run_type运行类型字段（1-正常跑批，2-自动重载），自动重载时暂不能限制时间
 *
 */
public class SparkAreaSplitXQ2 {

    /*
     * 管理区划拆分
     */

    public static void main(String[] args) {


        String pi_srcsys = args[0];
        String pi_table = args[1];
        String schema = args[2];
        String pi_startdate = args[3];
        String pi_enddate = args[4];
        String pi_sparkname = args[5];
        String pi_load_type = args[6];
        String pi_url = args[7];
        String pi_user = args[8];
        String pi_password = args[9];
        String pi_run_type = args[10];
        String pi_savemode = "append";
        if (args.length > 11)
            pi_savemode = args[11];
		/*
		String pi_srcsys = "LOFTA";
		String pi_batchno = "1";
		String pi_table = "tmp_ass_ta_shr_arsplt_spark";
		String schema = "ODS";
		String pi_startdate ="20040101";
		String pi_enddate = "20051201";
		String pi_sparkname = "spark_area_split2_1028";
		String pi_load_type ="A";
		*/
        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);

        Map<String, String> dBConOption = new HashMap<String, String>();
        //dBConOption.put("url", "jdbc:impala://10.64.36.70:25003/ods;AuthMech=3;LogLevel="+loglevel+";LogPath=/tmp/");
        dBConOption.put("url", pi_url);
        dBConOption.put("user", pi_user);
        dBConOption.put("password", pi_password);
        dBConOption.put("driver", "com.cloudera.impala.jdbc41.Driver");


        SparkSession hc = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .appName(pi_sparkname)
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();

        JdbcDialects.registerDialect(ImpalaDialect.getInstance());
        DataFrameReader dfRead = hc.read().format("jdbc").options(dBConOption);

        // 产品部用此部分
		/*String sql_schema=
				"( SELECT t.shr_chg_serialno,\n" +
				"		t.dk_tano,\n" +
				"		t.dk_cust_type,\n" + 
				"       t.area_rule_id,\n" + 
				"       t.dk_org_tree_of_branch,\n" + 
				"       t.sk_invpty_of_org_brnch,\n" + 
				"       t.dk_org_tree_of_bl,\n" + 
				"       t.sk_invpty_of_org_bl,\n" + 
				"       t.dk_custmngr_type,\n" + 
				"       t.sk_invpty_of_custmngr,\n" + 
				"       t.effective_from,\n" + 
				"       t.effective_to,\n" + 
				"       t.fa_cfm_ef,\n" + 
				"       t.fa_cfm_et, \n" + 
				"       t.div_ratio, \n" + 
				"       t.dk_system_of_sdata \n" + 
				"  FROM "+schema+".ass_ta_shr_arsplt t\n" + 
				" WHERE 1 = 2) as tt";
		*/
        //国海用此部分
        String sql_schema =
                "( SELECT t.shr_chg_serialno,\n" +
                        "		t.dk_tano,\n" +
                        "		t.dk_cust_type,\n" +
                        "       t.area_rule_id,\n" +
                        "       t.dk_org_tree_of_brnch,\n" +
                        "       t.sk_invpty_of_brnchorg,\n" +
                        "       t.dk_org_tree_of_bl,\n" +
                        "       t.sk_invpty_of_blorg,\n" +
                        "       t.dk_custmngr_type,\n" +
                        "       t.sk_invpty_of_custmngr,\n" +
                        "       t.effective_from,\n" +
                        "       t.effective_to,\n" +
                        "       t.fa_cfm_ef,\n" +
                        "       t.fa_cfm_et, \n" +
                        "       t.div_ratio, \n" +
                        "       t.dk_system_of_sdata \n" +
                        "  FROM " + schema + ".ass_ta_shr_arsplt t\n" +
                        " WHERE 1 = 2) as tt";

        System.out.println("================sql_schema====================");
        System.out.println(sql_schema);
        System.out.println("=================sql_schema===================");
        //StructType st =  dfRead.option("dbtable", sql_schema).load().schema();


        String sql_data =
                " (SELECT t.shr_chg_serialno,\n" +
                        "       cast(-1 as decimal(12,0)) AS sk_invpty_type,\n" +
                        "       t.sk_invpty_of_cust as sk_invpty,\n" +
                        "       cast(-1 as decimal(12,0)) AS sk_account_type,\n" +
                        "       t.sk_account_of_fd as sk_account,\n" +
                        //"       t5.sk_product_type AS sk_product_type,\n" +
                        "       cast(-1 as decimal(12,0)) AS sk_product_type,\n" +
                        "       t.sk_product,\n" +
                        "       t4.dk_agency_type AS dk_agency_type,\n" +
                        "       t.sk_agency,\n" +
                        "       t4.sk_region,\n" +
                        "       t.dk_tano,\n" +
                        "       nvl(t2.lval, cast(99999999999 as decimal(12,0))) AS lval_ag,\n" +
                        "       nvl(t2.rval, cast(0 as decimal(12,0))) AS rval_ag,\n" +
                        "       nvl(t3.lval, cast(99999999999 as decimal(12,0))) AS lval_rg,\n" +
                        "       nvl(t3.rval, cast(0 as decimal(12,0))) AS rval_rg,\n" +
                        "       greatest(nvl(t6.fa_bus_date, t.effective_from),\n" +
                        "                nvl(t2.effective_from, cast(0 as decimal(8,0))),\n" +
                        "                nvl(t3.effective_from, cast(0 as decimal(8,0)))) AS effective_from,\n" +
                        "       least(nvl(t7.fa_bus_date, t.effective_to),\n" +
                        "             nvl(t2.effective_to, cast(99991231 as decimal(8,0))),\n" +
                        "             nvl(t3.effective_to, cast(99991231 as decimal(8,0)))) AS effective_to,\n" +
                        "       nvl(t2.sk_agency_of_lv1, cast(-1 as decimal(12,0))) AS sk_agency_of_lv1,\n" +
                        "       nvl(t3.sk_region_of_lv1, cast(-1 as decimal(12,0))) AS sk_region_of_lv1,\n" +
                        "       ic.dk_cust_type,\n" +
                        //"       '*' as dk_cust_type,\n" +
                        "       t.effective_from AS fa_cfm_ef,\n" +
                        "       t.effective_to AS fa_cfm_et,\n" +
                        "       t.sk_tradeacco_reg,\n" +
                        "       t.agencyno,\n" +
                        "       t.netno,\n" +
                        "       t.dk_system_of_sdata,\n" +
                        "       t.dk_share_type\n" +
                        "  FROM  " + schema + ".ass_ta_fundbal_chg t\n" +
                        "  LEFT JOIN  " + schema + ".ip_cust ic\n" +
                        "    ON t.sk_invpty_of_cust = ic.sk_invpty_of_cust\n" +
                        "  LEFT JOIN  " + schema + ".ip_agency_hierarchy t2\n" +
                        "    ON t.sk_agency = t2.sk_agency\n" +
                        "   AND t2.dk_agency_tree = '01'\n" +
                        "  LEFT JOIN  " + schema + ".ip_agency t4\n" +
                        "    ON t.sk_agency = t4.sk_agency\n" +
                        "  LEFT JOIN  " + schema + ".comm_region_hierarchy t3\n" +
                        "    ON t4.sk_region = t3.sk_region\n" +
                        "   AND t3.dk_region_tree = '01'\n" +
                        //"  LEFT JOIN  "+schema+".prod_assoc_prod_type t5\n" +
                        //"    ON t.sk_product = t5.sk_product\n" +
                        //"   AND t5.dk_product_type_way = '01'\n" +
                        "  LEFT JOIN  " + schema + ".prod_nav t6\n" +
                        "    ON t.sk_product = t6.sk_product\n" +
                        "   AND t.effective_from = t6.fa_bus_date --t6.fa_cfm_date\n" +
                        "   AND t6.ta_nav_date IS NOT NULL\n" +
                        "  LEFT JOIN  " + schema + ".prod_nav t7\n" +
                        "    ON t.sk_product = t7.sk_product\n" +
                        "   AND t.effective_to = t7.fa_bus_date --t7.fa_cfm_date\n" +
                        "   AND t7.ta_nav_date IS NOT NULL\n" +
                        " WHERE ((t.effective_from >= " + pi_startdate + " AND\n" +
                        "       t.effective_from <=  " + pi_enddate + " ) OR\n" +
                        "       (t.effective_to >= " + pi_startdate + "  AND\n" +
                        "       t.effective_to <=  " + pi_enddate + " ) OR\n" +
                        "       ('" + pi_load_type + "' = 'A' AND  " + pi_startdate + "  >= t.effective_from AND\n" +
                        "       " + pi_startdate + "  < t.effective_to))\n" +
                        "   AND t.dk_system_of_sdata = '" + pi_srcsys + "' \n" +
                        "   AND t.effective_to > t.effective_from \n" +
                        // Modified By Luogong,20221110:去掉份额为零的数据
                        "   AND t.shares <> 0\n" +
                        // Modified By Luogong,20220927:支持自动重载
                        "   AND ('" + pi_run_type + "' = '1' OR ('" + pi_run_type + "' = '2' AND EXISTS (SELECT 1 FROM " + schema + ".org_areasplit_reload rd WHERE rd.reload_serialno = t.shr_chg_serialno AND rd.dk_reload_type='2'))) \n" +
                        //" ORDER BY sk_account, sk_invpty, sk_region_of_lv1, sk_agency_of_lv1 " +
                        " ) as tt";


        System.out.println("================sql_data====================");
        System.out.println(sql_data);
        System.out.println("=================sql_data===================");
        Dataset<Row> ipdf = dfRead.option("dbtable", sql_data).load();


        String sql_mapp = "( WITH a AS\n" +
                "  (SELECT t.area_rule_id,\n" +
                "          nvl(t.sk_product_type,   cast(-1 as decimal(12,0))) AS sk_product_type,\n" +
                "          nvl(t.sk_product,        cast(-1 as decimal(12,0))) AS sk_product,\n" +
                "          nvl(t.dk_cust_type,'*')                             AS dk_cust_type,\n" +
                "          nvl(sk_invpty_type,      cast(-1 as decimal(12,0))) AS sk_invpty_type,\n" +
                "          nvl(t.sk_invpty_of_cust, cast(-1 as decimal(12,0))) AS sk_invpty,\n" +
                "          nvl(t.sk_account_type,   cast(-1 as decimal(12,0))) AS sk_account_type,\n" +
                "          nvl(t.sk_account_of_fd,  cast(-1 as decimal(12,0))) AS sk_account,\n" +
                "          nvl(t.sk_agency,         cast(-1 as decimal(12,0))) AS sk_agency,\n" +
                "          nvl(t.dk_agency_type, '*')                          AS dk_agency_type,\n" +
                "          nvl(t.sk_region,         cast(-1 as decimal(12,0))) AS sk_region,\n" +
                "          t.dk_org_tree_of_branch  AS dk_org_tree,\n" +
                "          t.sk_invpty_of_org_brnch AS sk_org,\n" +
                "          t.dk_org_tree_of_bl,\n" +
                "          t.sk_invpty_of_org_bl    AS sk_org_of_bl,\n" +
                "          t.dk_custmngr_type,\n" +
                "          t.sk_invpty_of_custmngr,\n" +
                "          t.priority,\n" +
                "          t.div_ratio 				AS ratio,\n" +
                "          greatest(t.effective_from, "
                + "nvl(t2.effective_from, cast(0 as decimal(8,0))), "
                + "nvl(t3.effective_from, cast(0 as decimal(8,0))), "
                + "nvl(t4.effective_from, cast(0 as decimal(8,0))), "
                + "nvl(t5.effective_from, cast(0 as decimal(8,0)))) AS effective_from,\n" +
                "          least(t.effective_to, "
                + "nvl(t2.effective_to, cast(99991231 as decimal(8,0))), "
                + "nvl(t3.effective_to, cast(99991231 as decimal(8,0))), "
                + "nvl(t4.effective_to, cast(99991231 as decimal(8,0))), "
                + "nvl(t5.effective_to, cast(99991231 as decimal(8,0)))) AS effective_to,\n" +
                "          nvl(t2.lval, cast(-1 as decimal(12,0))) AS lval_ag,\n" +
                "          nvl(t2.rval, cast(999999999999 as decimal(12,0))) AS rval_ag,\n" +
                "          nvl(t3.lval, cast(-1           as decimal(12,0))) AS lval_rg,\n" +
                "          nvl(t3.rval, cast(999999999999 as decimal(12,0))) AS rval_rg,\n" +
                "          nvl(t5.lval, cast(-1           as decimal(12,0))) AS lval_ar,\n" +
                "          nvl(t5.rval, cast(999999999999 as decimal(12,0))) AS rval_ar,\n" +
                "          nvl(t4.lval, cast(-1 		  as decimal(12,0))) AS lval_bl,\n" +
                "          nvl(t4.rval, cast(999999999999 as decimal(12,0))) AS rval_bl,\n" +
                "          nvl(t.dk_tano,'*') 								 AS dk_tano,\n" +
                "          nvl(t.cserialno,'*') 							 AS cserialno,\n" +
                //"          nvl(t7.sk_tradeacco_reg,cast(-1 as decimal(12,0))) AS sk_tradeacco_reg,\n" +
                "          cast(-1 as decimal(12,0)) AS sk_tradeacco_reg,\n" +
                "          nvl(t.agencyno,'*') 								  AS agencyno,\n" +
                "          nvl(t.netno,'*') 								  AS netno,\n" +
                "          nvl(t.dk_share_type,'*') 						  AS dk_share_type,\n" +
                "          nvl(t2.sk_agency_of_lv1,-1)           AS sk_agency_of_lv1,\n" +
                "          nvl(t3.sk_region_of_lv1,-1)           AS sk_region_of_lv1,\n" +
                "          nvl(t.dk_arsplt_chg_flag,'0') AS dk_arsplt_chg_flag,\n" +
                "          t.dk_anal_dymic\n" +
                "   FROM " + schema + ".org_areasplit_common t\n" +
                "   LEFT JOIN " + schema + ".ip_agency_hierarchy t2 ON t.sk_agency = t2.sk_agency\n" +
                "   AND t2.dk_agency_tree = '01'\n" +
                "   LEFT JOIN " + schema + ".comm_region_hierarchy t3 ON t.sk_region = t3.sk_region\n" +
                "   AND t3.dk_region_tree = '01'\n" +
                "   LEFT JOIN " + schema + ".org_branch_hierarchy t4 ON t.sk_invpty_of_org_bl = t4.sk_invpty_of_org\n" +
                "   AND t.dk_org_tree_of_bl=t4.dk_org_tree\n" +
                "   LEFT JOIN " + schema + ".org_branch_hierarchy t5 ON t.sk_invpty_of_org_brnch = t5.sk_invpty_of_org\n" +
                "   AND t.dk_org_tree_of_branch=t5.dk_org_tree\n" +
                // Modified By Luogong,20221111:取有效规则
                "   WHERE t.effective_from <= " + pi_enddate + " AND t.effective_to >= " + pi_startdate + " \n" +
                //"   LEFT JOIN "+schema+".agrm_tradeaccount_ds t6 ON t.sk_tradeacco_reg =t6.sk_account_of_ds\n" +
                //"   LEFT JOIN "+schema+".agrm_tradeacco_reg t7 ON t6.bk_tradeaccount =t7.bk_tradeaccount\n" +
                //"   AND t6.agencyno=t7.agencyno \n"+
                " )\n" +
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
                "       t.dk_anal_dymic,\n" +
                "       t.sk_agency_of_lv1,\n" +
                "       t.sk_region_of_lv1\n" +
                "FROM a t\n" +
                //"ORDER BY t.priority,\n" +
                //"         t.sk_product DESC,\n" +
                //"         t.sk_agency_of_lv1 DESC,\n" +
                //"         t.sk_region_of_lv1 DESC " +
                " ) as tt";

        System.out.println("================sql_mapp====================");
        System.out.println(sql_mapp);
        System.out.println("=================sql_mapp===================");
        Dataset<Row> ddf = dfRead.option("dbtable", sql_mapp).load();

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


        JavaRDD<List<Row>> w = ipdf.toJavaRDD().map(new Function<Row, List<Row>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public List<Row> call(Row r) throws Exception {

                int v_cnt0, v_cnt1, v_cnt2, v_cnt3, v_cnt4, v_cnt5, v_cnt6;
                int v_count0, v_count1, v_count2, v_count3, v_count4, v_count5, v_count6;

                String vc_key = null;
                List<Row> ret = new ArrayList<Row>();

                Map<String, List<Row>> innerMap = new HashMap<String, List<Row>>();

                String pi_cserialno = null;//(String)r.getAs("cserialno");
                String pi_tano = null;//(String)r.getAs("dk_tano");
                String pi_dk_agency_type = (String) r.getAs("dk_agency_type");
                String pi_dk_cust_type = (String) r.getAs("dk_cust_type");
                String pi_agencyno = (String) r.getAs("agencyno");
                String pi_netno = (String) r.getAs("netno");
                String pi_dk_share_type = (String) r.getAs("dk_share_type");
                //String pi_reload_flag =(String)r.getAs("reload_flag");
                String pi_dk_system_of_sdata = (String) r.getAs("dk_system_of_sdata");
                String vc_pre_rule_id = null;
                BigDecimal vc_pre_priority = null;
                String vc_pre_dync_flag = null;

                BigDecimal pi_sk_prod_type = (BigDecimal) r.getAs("sk_product_type");
                BigDecimal pi_sk_product = (BigDecimal) r.getAs("sk_product");
                BigDecimal pi_sk_invpty_type = null;//(BigDecimal)r.getAs("sk_invpty_type");
                BigDecimal pi_sk_invpty = (BigDecimal) r.getAs("sk_invpty");
                BigDecimal pi_sk_account_type = null;//(BigDecimal)r.getAs("sk_account_type");
                BigDecimal pi_sk_account = (BigDecimal) r.getAs("sk_account");
                BigDecimal pi_sk_tradeacco_reg = (BigDecimal) r.getAs("sk_tradeacco_reg");
                BigDecimal pi_effective_from = (BigDecimal) r.getAs("effective_from");
                BigDecimal pi_effective_to = (BigDecimal) r.getAs("effective_to");
                //pi_trd_serialno     IN NUMBER

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
                vc_pre_rule_id = SparkSplitUtils.S_UNKNOWN;
                Iterator<Row> it = orMap.iterator();

                Row rule = null;
                BigDecimal priority = null;
                String area_rule_id = null;
                String dk_anal_dymic = null;
                BigDecimal ratio = null;

                int v_ef = 0;
                int v_et = 0;
                int v_rule_et = 0;
                int v_tmp_val = 0;
                //int v_dtrg_no=0;

                List<DateRange> vt_dt_range = new ArrayList<DateRange>();
                List<DateRange> vt_dt_range_pre = new ArrayList<DateRange>();
                List<DateRange> vt_dt_range_split = new ArrayList<DateRange>();
                vt_dt_range.add(new DateRange(pi_effective_from.intValue(), pi_effective_to.intValue(), 1.0d));

                int effective_from = 0;
                int effective_to = 0;
                List<Integer> vt_array_seq = new ArrayList<Integer>();
                while (it.hasNext()) {
                    rule = it.next();
                    vt_dt_range_split.clear();
                    effective_from = ((BigDecimal) rule.getAs("effective_from")).intValue();
                    effective_to = ((BigDecimal) rule.getAs("effective_to")).intValue();
                    DateRange dr = null;
                    for (int i = 0; i < vt_dt_range.size(); i++) {
                        dr = vt_dt_range.get(i);
                        v_ef = Math.max(dr.start_dt, effective_from);
                        if (effective_to < 99991231) {
                            v_rule_et = SparkSplitUtils.addDays(effective_to, 1);
                        } else {
                            v_rule_et = effective_to;
                        }
                        v_et = Math.min(dr.end_dt, v_rule_et);
                        v_accu_ratio = dr.remain_ratio;


                        //只取有效日期区间段
                        if (v_ef < v_et && v_accu_ratio > v_e) {
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
                                        pi_dk_cust_type,
                                        area_rule_id,
                                        rule.getAs("dk_org_tree"),
                                        rule.getAs("sk_org"),
                                        rule.getAs("dk_org_tree_of_bl"),
                                        rule.getAs("sk_org_of_bl"),
                                        rule.getAs("dk_custmngr_type"),
                                        rule.getAs("sk_invpty_of_custmngr"),
                                        new BigDecimal(v_ef),
                                        new BigDecimal(v_et),
                                        null,//fa_cfm_ef
                                        null,//fa_cfm_et
                                        new BigDecimal(v_ratio),//ratio
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
                                //v_accu_ratio = v_accu_ratio - v_ratio;
                                vt_dt_range_split.add(new DateRange(v_ef, v_et, v_accu_ratio - v_ratio));

                            }//end 多维度拆分
                        }// end 有效时间段
                    }//end for

                    if (vt_dt_range_split.size() > 0) {
                        vt_dt_range_pre.clear();
                        for (int m = 0; m < vt_dt_range.size(); m++) {
                            vt_array_seq.add(vt_dt_range.get(m).start_dt);
                            vt_array_seq.add(vt_dt_range.get(m).end_dt);
                        }
                        for (int m = 0; m < vt_dt_range_split.size(); m++) {
                            vt_array_seq.add(vt_dt_range_split.get(m).start_dt);
                            vt_array_seq.add(vt_dt_range_split.get(m).end_dt);
                        }

                        //对数组序列从小到排序，重新构造新的日期区间集合
                        v_count1 = vt_array_seq.size();
                        for (v_cnt1 = 0; v_cnt1 < v_count1; v_cnt1++) {
                            for (v_cnt2 = v_cnt1 + 1; v_cnt2 < v_count1; v_cnt2++) {
                                if (vt_array_seq.get(v_cnt2) < vt_array_seq.get(v_cnt1)) {
                                    v_tmp_val = vt_array_seq.get(v_cnt1);
                                    vt_array_seq.set(v_cnt1, vt_array_seq.get(v_cnt2));
                                    vt_array_seq.set(v_cnt2, v_tmp_val);
                                }
                            }
                            //重新构造日期区间
                            if (v_cnt1 >= 1 && vt_array_seq.get(v_cnt1) > vt_array_seq.get(v_cnt1 - 1)) {
                                vt_dt_range_pre.add(new DateRange(vt_array_seq.get(v_cnt1 - 1), vt_array_seq.get(v_cnt1), 0.0d));
                            }
                        } //end for v_cnt1

                        //计算新构造日期区间的剩余分摊比例
                        //v_count1  := vt_dt_range_pre.count;
                        //v_dtrg_no := 1;
                        for (v_cnt1 = 0; v_cnt1 < vt_dt_range_pre.size(); v_cnt1++) {
                            //v_count2 := vt_dt_range_split.count;
                            int v_found2 = 0;
                            //优先在本次拆分新拆分日期区间内查找剩余比例
                            for (v_cnt2 = 0; v_cnt2 < vt_dt_range_split.size(); v_cnt2++) {
                                if (vt_dt_range_pre.get(v_cnt1).start_dt >= vt_dt_range_split.get(v_cnt2).start_dt
                                        && vt_dt_range_pre.get(v_cnt1).start_dt < vt_dt_range_split.get(v_cnt2).end_dt
                                ) {
                                    vt_dt_range_pre.get(v_cnt1).remain_ratio = vt_dt_range_split.get(v_cnt2).remain_ratio;
                                    v_found2 = 1;
                                    break;
                                }
                            }
                            //若找不到，则在上次拆分日期区间内查找剩余比例
                            if (v_found2 == 0) {
                                //v_count2 := vt_dt_range.count;
                                for (v_cnt2 = 0; v_cnt2 < vt_dt_range.size(); v_cnt2++) {
                                    if (vt_dt_range_pre.get(v_cnt1).start_dt >= vt_dt_range.get(v_cnt2).start_dt
                                            && vt_dt_range_pre.get(v_cnt1).start_dt < vt_dt_range.get(v_cnt2).end_dt
                                    ) {
                                        vt_dt_range_pre.get(v_cnt1).remain_ratio = vt_dt_range.get(v_cnt2).remain_ratio;
                                        v_found2 = 1;
                                        break;
                                    }
                                }
                            }
                            //20170220 louyk modify 数组元素删除后，数组下标不连续的问题开始编号问题
                        }
                        vt_dt_range.clear();

                        //20161122 suncj modify 解决数组元素删除后，数组下标不从1开始编号问题
                        //vt_dt_range:=vt_dt_range_pre;
                        if (vt_dt_range_pre.size() > 0) {
                            int v_j = 0;
                            for (int v_i = 0; v_i < vt_dt_range_pre.size(); v_i++) {
                                //20170220 louyk modify 数组元素删除后，数组下标不连续的问题开始编号问题,使用IF过滤剩余占比为0的记录
                                if (vt_dt_range_pre.get(v_i).remain_ratio > v_e) {
                                    vt_dt_range.add(v_j, vt_dt_range_pre.get(v_i));//TODO set or add
                                    v_j = v_j + 1;
                                }//end if
                            }//end for
                        }//end if
                    }// end if cnt2>0
                    if (vt_dt_range.size() == 0) {
                        break;
                    }
                }// end while

                //处理拆分剩余比例

                for (v_cnt1 = 0; v_cnt1 < vt_dt_range.size(); v_cnt1++) {
                    ret.add(RowFactory.create(
                            r.getAs(0),//流水号，唯一值
                            pi_tano,//dk_tano
                            pi_dk_cust_type,//dk_org_tree
                            null,//area_rule_id
                            null,
                            new BigDecimal(-1),//sk_org
                            null,//dk_org_tree_of_bl
                            new BigDecimal(-1),//sk_org_of_bl
                            null,//dk_custmngr_type
                            new BigDecimal(-1),//sk_invpty_of_custmngr
                            new BigDecimal(vt_dt_range.get(v_cnt1).start_dt),//effective_from
                            new BigDecimal(vt_dt_range.get(v_cnt1).end_dt),//effective_to
                            null,//fa_cfm_ef
                            null,//fa_cfm_et
                            new BigDecimal(vt_dt_range.get(v_cnt1).remain_ratio),//ratio
                            pi_dk_system_of_sdata
                    ));
                }
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
        StructType st = dfRead.option("dbtable", sql_schema).load().schema();

        //hc.createDataFrame(x, st).show();

        hc.createDataFrame(x, st).write().format("parquet")
                .partitionBy("dk_system_of_sdata")
                .mode(pi_savemode.equalsIgnoreCase("append") ? SaveMode.Append : SaveMode.Overwrite).saveAsTable(schema + "." + pi_table);
        //hc.refreshTable(schema+"."+pi_table);
        hc.close();
    }
}
