package cn.com.businessmatrix.hive;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*
 *  累计交易客户数-金额分段
 *
 *  参数顺序：
 *
 *
 *
 */
public class SparkFltAssCustFundbalNonmnyBydayHive {


    public static void main(String[] args) throws Exception {

        String sparktable = args[0];//存放spark临时数据的表
        String startdate = args[1];
        String enddate = args[2];
        String schema_ods = args[3];
        String schema_pub = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String busdate = args[6];
        String srcsys = args[7];
        String savemode = "append";
        if (args.length > 8)
            savemode = args[8];


        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                //.config("hive.execution.engine", "spark")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.dynamic.partition", "true")
                //.config("spark.sql.parquet.writeLegacyFormat", "true")
                .enableHiveSupport()
                .appName(sparkname)
                //.master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();

        //取上一个工作日
        String vn_startdate = hc.sql("select t.last_wkdate from " + schema_pub + ".flt_comm_cnexchg_date t where t.sk_date=" + startdate)
                .collectAsList().get(0).getAs("last_wkdate").toString();
		    
		    /*
		    Row r=hc.sql("select min(t.effective_from_m) as vn_startmth ,max(t.effective_from_m)  as vn_endmth "+
		    			" from  "+schema_ods+".ass_ta_fundbal_chg t "+
		    			" INNER JOIN "+schema_pub+".flt_prod_nav t1 "+
		    			" ON t.sk_product = t1.sk_product "+
		    			" WHERE t1.bus_date BETWEEN least("+vn_startdate+",cast(concat(substr("+startdate+",1,6),'01') as int)) "+
		    		    " AND cast(concat(substr("+enddate+",1,6),'31') as int) "+
		    			" AND t1.fa_cfm_date >= t.effective_from "+
		    			" AND t1.fa_cfm_date < t.effective_to "+
		    			" and t.shares<>0 ")
				    .collectAsList().get(0);
		    String vn_startmth=r.getAs("vn_startmth").toString();
		    String vn_endmth=r.getAs("vn_endmth").toString();
		    */

        hc.sql("alter table " + schema_pub + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "',dk_is_momney='0')");
        System.out.println("alter table " + schema_pub + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "',dk_is_momney='0')");


        String sql_lookdf = "SELECT sk_invpty_of_cust ,dk_cust_type ,cust_name ,dk_id_type ,std_idno ,sk_region ,bk_region ,region_name ,sk_account_of_fd ,bk_fundaccount "
                + ",dk_tano ,bk_tradeaccount ,sk_agency ,agency_branch_name ,agencyno ,dk_sale_channel ,dk_agency_type ,agency_name ,netno ,sk_product ,bk_product "
                + ",product_cname ,bk_product_of_m ,prod_formal_code ,m_product_cname ,dk_share_type ,dk_prod_lv1_type ,dk_prod_lv2_type ,dk_prod_lv3_type "
                + ",sk_currency ,bk_currency ,currency_name ,dk_bourseflag ,bus_date ,cfm_date ,dk_capitalmode ,shares ,all_shares ,income ,lc_income ,frozen_shares "
                + ",frozen_income ,lc_frozen_income ,capital ,lc_capital ,all_capital ,lc_all_capital ,dk_system_of_upd ,nav_date ,sk_date_of_pre,bk_invpty_of_cust ,dk_system_of_sdata, dk_is_momney"
                + " from " + schema_pub + "." + sparktable
                + " where 1=2 ";

        String sql = " ( SELECT shr_chg_serialno ,"
                + " case "
                + " WHEN dk_system_of_sdata='ETF' THEN "
                + " 'B6' "
                + " WHEN dk_system_of_sdata='LOF' THEN "
                + " '98' "
                + " ELSE '05' "
                + " END AS dk_tano ,sk_invpty_of_cust ,sk_account_of_fd ,sk_tradeacco_reg ,sk_agency ,sk_product ,dk_share_type "
                + " ,sk_currency ,bk_fundaccount ,bk_tradeaccount ,bk_tradeaccount_of_ag ,agencyno ,netno ,dk_bourseflag ,effective_from "
                + " ,effective_to ,occur_shares ,shares ,occur_freeze ,freeze_share ,dk_agency_split_flag ,dk_is_valid ,sdata_serialno "
                + " ,memo ,ddvc ,dk_system_of_upd ,batchno ,inserttime ,updatetime ,dk_system_of_sdata  "
                + " FROM " + schema_ods + ".ass_ta_fundbal_chg_byday t "
                + " WHERE t.dk_system_of_sdata='" + srcsys + "'   AND t.shares<>0 )";

        sql = " SELECT /*+ BROADCAST(t1) */ t.sk_invpty_of_cust AS sk_invpty_of_cust ," +
                " t.sk_account_of_fd AS sk_account_of_fd  ," +
                " t.bk_fundaccount AS bk_fundaccount   ," +
                " t.dk_tano AS dk_tano  ," +
                " t.SK_TRADEACCO_REG AS SK_TRADEACCO_REG   ," +
                " t.bk_tradeaccount AS bk_tradeaccount  ," +
                " t.sk_agency AS sk_agency  ," +
                " t.agencyno AS agencyno  ," +
                " t.netno AS netno  ," +
                " t.sk_product AS sk_product ," +
                " t.dk_share_type AS dk_share_type  ," +
                " t.dk_bourseflag AS dk_bourseflag   ," +
                " t1.bus_date AS bus_date  ," +
                " t1.fa_cfm_date AS cfm_date   ," +
                " t.shares AS shares   ," +
                " t.shares AS all_shares  ," +
                " 0 AS income   ," +
                " 0 AS lc_income   ," +
                " t.freeze_share AS frozen_shares  ," +
                " 0 AS frozen_income   ," +
                " 0 AS lc_frozen_income  ," +
                " t.shares * t1.net_value AS capital   ," +
                " t.shares * t1.net_value AS lc_capital   ," +
                " t.shares * t1.net_value AS all_capital   ," +
                " t.shares * t1.net_value AS lc_all_capital   ," +
                " t.dk_system_of_sdata AS dk_system_of_sdata   ," +
                " t.dk_system_of_upd AS dk_system_of_upd   ," +
                " t1.ta_nav_date AS nav_date ," +
                " regexp_replace(cast(date_format(date_sub(from_unixtime(unix_timestamp(cast(t1.bus_date AS string),'yyyyMMdd') ,'yyyy-MM-dd HH:mm:ss'),1),'yyyyMMdd') AS INT),'-','') AS sk_date_of_pre " +
                " FROM " + sql + " t " +
                " INNER JOIN  " +
                " 	(SELECT t1.sk_product,t1.ta_nav_date,t1.bus_date,t1.fa_cfm_date,t1.net_value " +
                " 	FROM " + schema_pub + ".flt_prod_nav t1 " +
                " 	INNER JOIN " + schema_pub + ".flt_prod_product t2 " +
                " 		ON t1.sk_product=t2.sk_product " +
                " 			AND t2.bus_date=" + busdate +
                " 	WHERE t1.bus_date " +
                " 		BETWEEN least(" + vn_startdate + "," + startdate + ") " +
                " 			AND  " + enddate +
                " 			AND " +
                " 		CASE " +
                " 		WHEN nvl(t2.dk_product_type,'0') NOT IN('02','03') THEN " +
                " 		'1' " +
                " 		WHEN t1.bus_date<=nvl(t2.due_date,99991231) " +
                " 			AND t2.dk_product_type IN('02','03') THEN " +
                " 		'1' " +
                " 		ELSE '0' " +
                " 		END ='1' ) t1 " +
                " 	ON t.sk_product = t1.sk_product " +
                " WHERE t1.fa_cfm_date >= t.effective_from " +
                " 		AND t1.fa_cfm_date < t.effective_to  ";

        //第3层


        String sql2 = "SELECT /*+ BROADCAST(t3), BROADCAST(t4), BROADCAST(t7) */ " +
                " t8.sk_invpty_of_cust ," +
                " t2.dk_cust_type ," +
                " t2.cust_name  ," +
                " t2.dk_id_type  ," +
                " t2.std_idno  ," +
                " t3.sk_region  ," +
                " t4.bk_region ," +
                " t4.region_name ," +
                " t.sk_account_of_fd  ," +
                " t.bk_fundaccount ," +
                " t.dk_tano  ," +
                " t.bk_tradeaccount   ," +
                " t.sk_agency ," +
                " t3.agency_branch_name   ," +
                " t.agencyno  ," +
                " t3.dk_sale_channel ," +
                " t3.dk_agency_type  ," +
                " t3.agency_name   ," +
                " t.netno  ," +
                " t.sk_product  ," +
                " t7.bk_product  ," +
                " t7.product_cname   ," +
                " t7.bk_product_of_m  ," +
                " t7.prod_formal_code  ," +
                " t7.m_product_cname  ," +
                " t.dk_share_type   ," +
                " t7.dk_prod_lv1_type  ," +
                " t7.dk_prod_lv2_type  ," +
                " t7.dk_prod_lv3_type  ," +
                " t7.sk_currency AS sk_currency  ," +
                " t7.bk_currency AS bk_currency  ," +
                " t7.currency_name AS currency_name  ," +
                " t.dk_bourseflag AS dk_bourseflag  ," +
                " cast(t.bus_date  as decimal(8,0)) AS bus_date  ," +
                " cast(t.cfm_date  as decimal(8,0)) AS cfm_date ," +
                " t8.dk_capitalmode AS dk_capitalmode ," +
                " t.shares AS shares  ," +
                " t.shares AS all_shares  ," +
                " cast(0 as decimal(21,6)) AS income  ," +
                " cast(0 as decimal(21,6)) AS lc_income  ," +
                " t.frozen_shares AS frozen_shares ," +
                " cast(0 as decimal(21,6)) AS frozen_income  ," +
                " cast(0 as decimal(21,6)) AS lc_frozen_income  ," +
                " t.capital AS capital  ," +
                " case  WHEN t7.sk_currency=1 THEN t.lc_capital END AS lc_capital  ," +
                " t.all_capital AS all_capital  ," +
                " case WHEN t7.sk_currency=1 THEN t.lc_all_capital  END AS lc_all_capital  ," +
                " t.dk_system_of_upd AS dk_system_of_upd  ," +
                " cast(t.nav_date as decimal(8,0)) AS nav_date ," +
                " cast(t.sk_date_of_pre as decimal(8,0)) as sk_date_of_pre ," +
                " t2.bk_invpty_of_cust AS bk_invpty_of_cust," +
                " t.dk_system_of_sdata AS dk_system_of_sdata  ," +
                " cast('0' as string) as dk_is_momney  " +
                " FROM temp_register_spark t " +
                " LEFT JOIN " + schema_ods + ".ip_agency t3 " +
                " ON t.sk_agency = t3.sk_agency  " +
                " LEFT JOIN " + schema_ods + ".comm_region t4 " +
                " 	ON t4.sk_region =t3.sk_region " +
                " LEFT JOIN  " +
                " (SELECT sk_product , " +
                " bk_product   ," +
                " product_cname   ," +
                " bk_product_of_m  ," +
                " prod_formal_code   ," +
                " m_product_cname   ," +
                " dk_prod_lv1_type   ," +
                " dk_prod_lv2_type   ," +
                " dk_prod_lv3_type  ," +
                " sk_currency   ," +
                " bk_currency   ," +
                " currency_name " +
                " FROM " + schema_pub + ".flt_prod_product " +
                " WHERE bus_date=" + busdate +
                " ) t7 " +
                " ON t.sk_product = t7.sk_product  " +
                " LEFT JOIN  " +
                " (SELECT sk_tradeacco_reg , " +
                " sk_invpty_of_cust , " +
                " dk_capitalmode AS dk_capitalmode  " +
                " FROM " + schema_pub + ".flt_agrm_tradeacco_reg " +
                " WHERE bus_date=" + busdate + " ) t8  " +
                " ON t.sk_tradeacco_reg=t8.sk_tradeacco_reg  " +
                " LEFT JOIN  " +
                " (SELECT t.sk_invpty_of_cust AS sk_invpty_of_cust  ," +
                " t.bk_invpty_of_cust AS bk_invpty_of_cust  ," +
                " t.dk_cust_type AS dk_cust_type  ," +
                " t.cust_name AS cust_name  ," +
                " t.dk_id_type AS dk_id_type  ," +
                " t.idno AS idno  ," +
                " t.std_idno AS std_idno " +
                " FROM " + schema_pub + ".flt_ip_custinfo t " +
                " WHERE t.bus_date=" + busdate + " ) t2 " +
                " ON t8.sk_invpty_of_cust = t2.sk_invpty_of_cust";


        System.out.println(sql_lookdf);
        System.out.println();
        System.out.println(sql);
        System.out.println();
        System.out.println(sql2);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        Dataset<Row> adf = hc.sql(sql);
        adf.repartition(1000, adf.col("sk_invpty_of_cust")).registerTempTable("temp_register_spark");

        Dataset<Row> adf2 = hc.sql(sql2);
        hc.createDataFrame(adf2.javaRDD(), lookdf.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata", "dk_is_momney")
                .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                .saveAsTable(schema_pub + "." + sparktable);

        hc.close();

    }

}
