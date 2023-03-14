package cn.com.businessmatrix.hive;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*
 *  交易
 *
 *  参数顺序：
 *
 *
 *
 */
public class SparkFltTrdSaleTranHive {


    public static void main(String[] args) throws Exception {

        String sparktable = args[0];//存放spark临时数据的表
        String startdate = args[1];
        String enddate = args[2];
        String schema_pub = args[3];
        String schema_ods = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String busdate = args[6];
        String savemode = "append";
        if (args.length > 7)
            savemode = args[7];


        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
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

        String sql_lookdf = "select * from tmp." + sparktable + " where 1=2 ";

        String sql = "select /*+ BROADCAST(t3), BROADCAST(t4), BROADCAST(t5), BROADCAST(t6) */ t1.trd_st_trx_serialno " +
                ",t1.sk_invpty_of_cust " +
                ",t2.dk_cust_type " +
                ",t2.cust_name " +
                ",t2.dk_id_type " +
                ",t2.std_idno " +
                ",t3.sk_region " +
                ",t3.bk_region " +
                ",t3.region_name " +
                ",t1.sk_account_of_fd " +
                ",t1.bk_fundaccount " +
                ",t1.cserialno " +
                ",t1.dk_tano " +
                ",t1.sk_tradeacco_reg " +
                ",t1.bk_tradeaccount " +
                ",t1.sk_agency " +
                ",t3.agency_branch_name " +
                ",t1.agencyno " +
                ",t3.dk_sale_channel " +
                ",t3.dk_agency_type " +
                ",t3.agency_name " +
                ",t1.netno " +
                ",t4.sk_product " +
                ",t4.bk_product " +
                ",t4.product_cname " +
                ",t4.bk_product_of_m " +
                ",t4.prod_formal_code " +
                ",t4.m_product_cname " +
                ",t1.dk_share_type " +
                ",t4.dk_prod_lv1_type " +
                ",t4.dk_prod_lv2_type " +
                ",t4.dk_prod_lv3_type " +
                ",t5.sk_currency " +
                ",t5.bk_currency " +
                ",t5.currency_name " +
                ",t1.sk_mkt_trade_type    " +
                ",t6.bk_mkt_trade_type    " +
                ",t6.mkt_trade_type_desc " +
                ",t1.request_date " +
                ",t1.ta_cfm_date " +
                ",t1.dk_bourseflag " +
                ",t1.requestno " +
                ",t1.net_value " +
                ",t1.dk_bonus_type " +
                ",t1.req_balance " +
                ",t1.req_balance  as lc_req_balance " +
                ",t1.req_shares " +
                ",t1.confirm_balance " +
                ",t1.confirm_balance  as lc_confirm_balance " +
                ",t1.confirm_shares " +
                ",t1.interest " +
                ",t1.interest  as lc_interest " +
                ",t1.interest_tax " +
                ",t1.interest_tax  as lc_interest_tax " +
                ",t1.interest_share " +
                ",t1.total_fee " +
                ",t1.total_fee  as lc_total_fee " +
                ",t1.trade_fee " +
                ",t1.trade_fee  as lc_trade_fee " +
                ",t1.ta_fee " +
                ",t1.ta_fee  as lc_ta_fee " +
                ",t1.agency_fee " +
                ",t1.agency_fee  as lc_agency_fee " +
                ",t1.regist_fee " +
                ",t1.regist_fee  as lc_regist_fee " +
                ",t1.fund_fee " +
                ",t1.fund_fee  as lc_fund_fee " +
                ",t1.stamp_duty " +
                ",t1.stamp_duty  as lc_stamp_duty " +
                ",t1.back_fee " +
                ",t1.back_fee  as lc_back_fee " +
                ",t1.agency_backfare " +
                ",t1.agency_backfare  as lc_agency_backfare " +
                ",t1.manager_backfare " +
                ",t1.manager_backfare  as lc_manager_backfare " +
                ",t1.fund_backfare " +
                ",t1.fund_backfare  as lc_fund_backfare " +
                ",t1.other_fee " +
                ",t1.other_fee  as lc_other_fee " +
                ",t1.return_fee " +
                ",t1.return_fee  as lc_return_fee " +
                ",t1.dk_saletran_status " +
                ",t1.dk_retcode " +
                ",t1.bk_product_of_tgt " +
                ",t1.dk_share_type_of_tgt " +
                ",t1.bk_fundaccount_of_tgt " +
                ",t1.tgt_agencyno " +
                ",t1.tgt_netno " +
                ",t1.dk_businflag " +
                ",t1.dk_outbusinflag " +
                ",t1.perf_pay " +
                ",t1.perf_pay as lc_perf_pay " +
                ",t1.childnetno " +
                ",t1.dk_system_of_upd " +
                ",cast(0 as decimal(12,0)) " +
                ",cast(current_timestamp() as string) as inserttime " +
                ",cast(current_timestamp() as string) as updatetime " +
                ",t2.bk_invpty_of_cust " +
                ",t1.dk_system_of_sdata " +
                " ,t1.ta_cfm_date_m " +
                " from tmp.pub_trd_ta_saletran_bucket16 t1 " +
                " left join " +
                " (select  " +
                " 		dk_cust_type " +
                " 		,cust_name " +
                " 		,dk_id_type " +
                " 		,std_idno " +
                "  		,bk_invpty_of_cust " +
                " 		,sk_invpty_of_cust " +
                "  		from tmp.flt_ip_custinfo_bucket64 " +
                "  	where bus_date=" + busdate + ") t2 " +
                "  	on t1.sk_invpty_of_cust = t2.sk_invpty_of_cust " +
                "  	left join  " + schema_pub + ".flt_ip_agency t3 " +
                "  	on t1.sk_agency = t3.sk_agency " +
                "  	left join (select " +
                "  		sk_product " +
                "  		,bk_product " +
                "  		,product_cname " +
                "  		,bk_product_of_m " +
                " 		,prod_formal_code " +
                " 		,m_product_cname " +
                " 		,dk_prod_lv1_type " +
                " 		,dk_prod_lv2_type " +
                " 		,dk_prod_lv3_type " +
                " 		from  " + schema_pub + ".flt_prod_product " +
                " 		where bus_date=" + busdate +
                " 	) t4 " +
                " 		on t1.sk_product = t4.sk_product " +
                " 		left join " + schema_pub + ".flt_comm_currency t5 " +
                " 			on t1.sk_currency = t5.sk_currency " +
                "		left join " + schema_ods + ".comm_mkt_tradetype t6 " +
                "			on t1.sk_mkt_trade_type = t6.sk_mkt_trade_type " +
                " 		where t1.ta_cfm_date_m between floor (" + startdate + "/100) and floor (" + enddate + "/100)";


        System.out.println(sql_lookdf);
        System.out.println();
        System.out.println(sql);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        Dataset<Row> adf = hc.sql(sql);

        hc.createDataFrame(adf.javaRDD(), lookdf.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata", "cfm_date_m")
                .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                .saveAsTable("tmp." + sparktable);


        //hc.sql(sql2);

        hc.close();

    }

}
