package cn.com.businessmatrix.hive;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*

 */
public class SparkTrdSaleTranHive {


    public static void main(String[] args) throws Exception {

        String sparktable1 = args[0];
        String sparktable2 = args[1];

        String startdate = args[2];
        String enddate = args[3];

        String schema_stg = args[4];
        String schema_ods = args[5];

        String sparkname = args[6];//spark任务名称，方便查找
        String srcsys = args[7];
        String batchno = args[8];

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

        String sql_lookdf1 = "select * from tmp." + sparktable1 + " where 1=2 ";
        String sql_lookdf2 = "select * from tmp." + sparktable2 + " where 1=2 ";

        String sql1 = "SELECT  /*+ BROADCAST(t1),BROADCAST(m),BROADCAST(t3),BROADCAST(t4) */  t.trd_st_trx_serialno--交易流水号\n" +
                "      ,t.sk_invpty_of_cust--客户参与者代理键\n" +
                "      ,t.dk_cust_type--客户类型\n" +
                "      ,t.sk_account_of_fd--基金账户代理键\n" +
                "      ,t.sk_tradeacco_reg--ta登记交易账户代理键\n" +
                "      ,NVL(t3.sk_agency,t4.sk_agency) as sk_agency--销售渠道代理键\n" +
                "      ,t.sk_product--产品代理键\n" +
                "      ,nvl(m.dict_code,concat('~',t.dk_share_type)) as dk_share_type--收费模式\n" +
                "      ,t.sk_currency--结算币种\n" +
                "      ,t.sk_agreement_of_ration--定投协议代理键\n" +
                "      ,t.sk_mkt_trade_type--基金销售交易类型代理键\n" +
                "      ,t.bk_fundaccount--基金帐号\n" +
                "      ,t.bk_tradeaccount--交易账号\n" +
                "      ,t.cserialno--交易确认流水号\n" +
                "      ,t.dk_tano--ta代码\n" +
                "      ,t.agencyno--销售机构代码\n" +
                "      ,t.netno--托管网点号码\n" +
                "      ,cast(t.transaction_date as decimal(8,0)) as transaction_date --交易发生日期\n" +
                "      ,cast(t.request_date     as decimal(8,0)) as request_date --申请日期\n" +
                "      ,t.req_balance--申请金额\n" +
                "      ,t.req_shares--申请份额\n" +
                "      ,t.confirm_balance--确认金额\n" +
                "      ,t.confirm_shares--确认份额\n" +
                "      ,t.interest--利息\n" +
                "      ,t.interest_tax--利息税\n" +
                "      ,t.total_fee--总费用\n" +
                "      ,t.trade_fee--手续费\n" +
                "      ,t.ta_fee--过户费\n" +
                "      ,t.agency_fee--代销费\n" +
                "      ,t.regist_fee--注册登记人费\n" +
                "      ,t.fund_fee--归基金资产费\n" +
                "      ,t.stamp_duty--印花税\n" +
                "      ,t.back_fee--后收费用\n" +
                "      ,t.agency_backfare--后收费归销售机构\n" +
                "      ,t.manager_backfare--后收费归管理人\n" +
                "      ,t.fund_backfare--后收费归基金资产\n" +
                "      ,t.other_fee--其他费用\n" +
                "      ,t.return_fee--返还保费\n" +
                "      ,t.interest_share--认购期利息转份额\n" +
                "      ,t.net_value--成交单位净值\n" +
                "      ,t.dk_saletran_status--交易处理返回代码\n" +
                "      ,t.dk_retcode--失败原因代码\n" +
                "      ,t.dk_ta_flag--ta发起标志\n" +
                "      ,t.requestno--销售商申请编号\n" +
                "      ,t.agio--实际折扣率\n" +
                "      ,t.dk_bonus_type--分红方式\n" +
                "      ,t.dk_exceedflag--巨额赎回处理标志\n" +
                "      ,t.childnetno--交易网点号码\n" +
                "      ,t.dk_freeze_cause--冻结原因\n" +
                "      ,cast(t.freeze_end_date as decimal(8,0)) as freeze_end_date --冻结截止日期\n" +
                "      ,t.protocolno--销售机构定投协议号\n" +
                "      ,t.dk_bourseflag--交易来源\n" +
                "      ,t.back_fare_agio--后端申购费折扣\n" +
                "      ,t.fare_ratio--交易费率\n" +
                "      ,t.perf_pay--业绩报酬\n" +
                "      ,t.penalty_fee--违约金\n" +
                "      ,t.trade_fee_income--交易手续费收入\n" +
                "      ,null as  ag_split_ruleno--渠道拆分规则序号\n" +
                "      ,t.ip_addr--ip地址\n" +
                "      ,t.sk_product_of_tgt as sk_product_of_tgt--对方产品代理键\n" +
                "      ,t.bk_product_of_tgt as bk_product_of_tgt--对方产品代码\n" +
                "      ,t.dk_share_type_of_tgt as dk_share_type_of_tgt--份额类型\n" +
                "      ,t.sk_account_of_fd_tgt as sk_account_of_fd_tgt--对方基金账户代理键\n" +
                "      ,t.bk_fundaccount_of_tgt as bk_fundaccount_of_tgt--对方基金账号\n" +
                "      ,t.tgt_agencyno as tgt_agencyno--对方销售机构代码\n" +
                "      ,t.tgt_netno as tgt_netno--对方管理网点代码\n" +
                "      ,t.dk_businflag as dk_businflag--TA内部业务代码\n" +
                "      ,t.dk_outbusinflag as dk_outbusinflag--销售机构业务代码\n" +
                "      ,t.dk_is_valid--是否有效1-是，0-否\n" +
                "      ,t.dk_system_of_upd--源记录唯一标识指记录首次进入时源记录标识\n" +
                "      ,t.sdata_serialno--备注\n" +
                "      ,t.memo--数据校验码\n" +
                "      ,t.ddvc--数据更新系统\n" +
                "      ,cast(t.batchno as decimal(16,0)) as batchno --批次号\n" +
                "      ,t.inserttime--数据插入时间戳\n" +
                "      ,t.updatetime--数据插入时间戳\n" +
                "      ,null as request_time\n" +
                "      ,cast(t.ta_cfm_date   as decimal(8,0)) as ta_cfm_date--\n" +
                "      ,t.dk_system_of_sdata--\n" +
                "      ,cast(substr(t.ta_cfm_date,1,6)  as decimal(6,0)) as ta_cfm_date_m--\n" +
                "  FROM (SELECT  " +
                //"         CAST(default.genseq('seq_trd_st_trx_serialno','20000','1',t.bk_fundaccount) AS DECIMAL(30, 0)) AS \n" +
                " 				  null as trd_st_trx_serialno--交易流水号\n" +
                "                ,t.sk_invpty_of_cust--客户参与者代理键\n" +
                "                ,t.dk_cust_type--客户类型\n" +
                "                ,t.sk_account_of_fd--基金账户代理键\n" +
                "                ,t.sk_tradeacco_reg--ta登记交易账户代理键\n" +
                "                ,t.sk_agency--销售渠道代理键\n" +
                "                ,CAST(t.sk_product AS DECIMAL(12, 0)) AS sk_product--产品代理键\n" +
                "                ,t.dk_share_type--收费模式\n" +
                "                ,CAST(t.sk_currency AS DECIMAL(12, 0)) AS sk_currency--结算币种\n" +
                "                ,CAST(t.sk_agreement_of_ration AS DECIMAL(12, 0)) AS sk_agreement_of_ration--定投协议代理键\n" +
                "                ,t.sk_mkt_trade_type--基金销售交易类型代理键\n" +
                "                ,t.bk_fundaccount--基金帐号\n" +
                "                ,t.bk_tradeaccount--交易账号\n" +
                "                ,t.cserialno--交易确认流水号\n" +
                "                ,t.dk_tano--ta代码\n" +
                "                ,t.agencyno--销售机构代码\n" +
                "                ,t.netno--托管网点号码\n" +
                "                ,t.transaction_date--交易发生日期\n" +
                "                ,CAST(t.request_date AS DECIMAL(8, 0)) AS request_date--申请日期\n" +
                "                ,CAST(t.req_balance AS DECIMAL(21, 6)) AS req_balance--申请金额\n" +
                "                ,CAST(t.req_shares AS DECIMAL(17, 2)) AS req_shares--申请份额\n" +
                "                ,CAST(t.confirm_balance AS DECIMAL(17, 2)) AS confirm_balance--确认金额\n" +
                "                ,t.confirm_shares--确认份额\n" +
                "                ,t.interest--利息\n" +
                "                ,CAST(t.interest_tax AS DECIMAL(21, 6)) AS interest_tax--利息税\n" +
                "                ,t.total_fee--总费用\n" +
                "                ,CAST(t.trade_fee AS DECIMAL(21, 6)) AS trade_fee--手续费\n" +
                "                ,CAST(t.ta_fee AS DECIMAL(21, 6)) AS ta_fee--过户费\n" +
                "                ,CAST(t.agency_fee AS DECIMAL(21, 6)) AS agency_fee--代销费\n" +
                "                ,CAST(t.regist_fee AS DECIMAL(21, 6)) AS regist_fee--注册登记人费\n" +
                "                ,CAST(t.fund_fee AS DECIMAL(21, 6)) AS fund_fee--归基金资产费\n" +
                "                ,CAST(t.stamp_duty AS DECIMAL(21, 6)) AS stamp_duty--印花税\n" +
                "                ,CAST(t.back_fee AS DECIMAL(21, 6)) AS back_fee--后收费用\n" +
                "                ,t.agency_backfare--后收费归销售机构\n" +
                "                ,CAST(t.manager_backfare AS DECIMAL(21, 6)) AS manager_backfare--后收费归管理人\n" +
                "                ,t.fund_backfare--后收费归基金资产\n" +
                "                ,CAST(t.other_fee AS DECIMAL(21, 6)) AS other_fee--其他费用\n" +
                "                ,CAST(t.return_fee AS DECIMAL(21, 6)) AS return_fee--返还保费\n" +
                "                ,CAST(t.interest_share AS DECIMAL(17, 2)) AS interest_share--认购期利息转份额\n" +
                "                ,t.net_value--成交单位净值\n" +
                "                ,t.dk_saletran_status--交易处理返回代码\n" +
                "                ,t.dk_retcode--失败原因代码\n" +
                "                ,t.dk_ta_flag--ta发起标志\n" +
                "                ,t.requestno--销售商申请编号\n" +
                "                ,CAST(t.agio AS DECIMAL(12, 6)) AS agio--实际折扣率\n" +
                "                ,t.dk_bonus_type--分红方式\n" +
                "                ,t.dk_exceedflag--巨额赎回处理标志\n" +
                "                ,t.childnetno--交易网点号码\n" +
                "                ,t.dk_freeze_cause--冻结原因\n" +
                "                ,CAST(t.freeze_end_date AS DECIMAL(8, 0)) AS freeze_end_date--冻结截止日期\n" +
                "                ,CAST(t.protocolno AS STRING) AS protocolno--销售机构定投协议号\n" +
                "                ,t.dk_bourseflag--交易来源\n" +
                "                ,CAST(t.back_fare_agio AS DECIMAL(6, 4)) AS back_fare_agio--后端申购费折扣\n" +
                "                ,t.fare_ratio--交易费率\n" +
                "                ,t.perf_pay--业绩报酬\n" +
                "                ,t.penalty_fare as penalty_fee--违约金\n" +
                "                ,t.self_trade_fee as trade_fee_income--交易手续费收入\n" +
                "                ,t.ag_split_ruleno--渠道拆分规则序号\n" +
                "                ,t.ip_addr--ip地址\n" +
                "                ,t.sk_product_of_tgt--对方产品代理键\n" +
                "                ,t.bk_product_of_tgt--对方产品代码\n" +
                "                ,t.dk_share_type_of_tgt--份额类型\n" +
                "                ,t.sk_account_of_fd_tgt--对方基金账户代理键\n" +
                "                ,t.bk_fundaccount_of_tgt--对方基金账号\n" +
                "                ,t.tgt_agencyno--对方销售机构代码\n" +
                "                ,t.tgt_netno--对方管理网点代码\n" +
                "                ,t.c_businflag as dk_businflag--TA内部业务代码\n" +
                "                ,t.c_outbusinflag as dk_outbusinflag--销售机构业务代码\n" +
                "                ,t.dk_is_valid--是否有效1-是，0-否\n" +
                "                ,t.sdata_serialno--源记录唯一标识指记录首次进入时源记录标识\n" +
                "                ,t.memo--备注\n" +
                "                ,t.ddvc--数据校验码\n" +
                "                ,t.dk_system_of_upd--数据更新系统\n" +
                "                ,t.batchno--批次号\n" +
                "                ,cast(current_timestamp() as string) AS inserttime--数据插入时间戳\n" +
                "                ,cast(current_timestamp() as string) AS updatetime--数据插入时间戳\n" +
                "                ,t.ta_cfm_date\n" +
                "                ,0 AS areacode_split\n" +
                "                ,0 AS netno_split\n" +
                "                ,nvl(t.c_bourseflag, '9') AS marketioflag\n" +
                "                ,t.sk_agency_of_ds_acct\n" +
                "                ,t.capitalmode\n" +
                "                ,t.open_date\n" +
                "                ,t.bk_region\n" +
                "                ,lpad(t.agencyno, 3, '#') AS agencyno_split\n" +
                "                ,NULL AS requestno_split\n" +
                "                ,t.c_sharetype--份额类别\n" +
                "                ,t.c_fundcode--基金代码\n" +
                "                ,t.dk_system_of_sdata\n" +
                "            FROM (SELECT /*+ BROADCAST(ds),BROADCAST(m1),BROADCAST(m2),BROADCAST(m3),BROADCAST(m4),BROADCAST(m5),BROADCAST(m6) */ \n" +
                "                           t1.sk_invpty AS sk_invpty_of_cust\n" +
                "                          ,t1.dk_cust_type AS dk_cust_type\n" +
                "                          ,t1.sk_account AS sk_account_of_fd\n" +
                "                          ,t1.sk_tradeacco_reg AS sk_tradeacco_reg\n" +
                "                          ,ds.sk_agency AS sk_agency\n" +
                "                          ,nvl(t1.sk_product, -1) AS sk_product\n" +
                "                          ,t1.dk_share_type AS dk_share_type\n" +
                "                          ,nvl(t1.sk_currency, -1) AS sk_currency\n" +
                "                          ,nvl(sa.sk_agreement_of_ration, -1) AS sk_agreement_of_ration\n" +
                "                          ,t1.sk_mkt_trade_type\n" +
                "                          ,t1.c_fundacco AS bk_fundaccount\n" +
                "                          ,t1.c_tradeacco AS bk_tradeaccount\n" +
                "                          ,t1.c_cserialno AS cserialno\n" +
                "                          ,t1.tano AS dk_tano\n" +
                "                          ,t1.c_agencyno AS agencyno\n" +
                "                          ,nvl(t1.c_netno, t1.c_agencyno) AS netno\n" +
                "                          ,NULL AS transaction_date\n" +
                "                          ,CAST(t1.d_date AS INT) AS request_date\n" +
                "                          ,t1.f_balance AS req_balance\n" +
                "                          ,t1.f_shares AS req_shares\n" +
                "                          ,t1.f_confirmbalance AS confirm_balance\n" +
                "                          ,t1.f_confirmshares AS confirm_shares\n" +
                "                          ,t1.f_interest AS interest\n" +
                "                          ,t1.f_interesttax AS interest_tax\n" +
                "                          ,t1.f_totalfare AS total_fee\n" +
                "                          ,t1.f_tradefare AS trade_fee\n" +
                "                          ,t1.f_tafare AS ta_fee\n" +
                "                          ,t1.f_agencyfare AS agency_fee\n" +
                "                          ,t1.f_registfare AS regist_fee\n" +
                "                          ,t1.f_fundfare AS fund_fee\n" +
                "                          ,t1.f_stamptax AS stamp_duty\n" +
                "                          ,t1.f_backfare AS back_fee\n" +
                "                          ,t1.f_backfare4agt AS agency_backfare\n" +
                "                          ,t1.f_backfare - t1.f_backfare4agt - t1.f_backfare4fund AS manager_backfare\n" +
                "                          ,t1.f_backfare4fund AS fund_backfare\n" +
                "                          ,t1.f_otherfare1 AS other_fee\n" +
                "                          ,t1.f_returnfare AS return_fee\n" +
                "                          ,t1.f_interestshare AS interest_share\n" +
                "                          ,t1.f_netvalue AS net_value\n" +
                "                          ,CASE\n" +
                "                             WHEN c_status = '0000' THEN\n" +
                "                              '1'\n" +
                "                             ELSE\n" +
                "                              '0'\n" +
                "                           END AS dk_saletran_status -- 新增字典转换逻辑\n" +
                "                          ,nvl(m3.dict_code, concat('~', t1.c_cause)) AS dk_retcode -- 新增字典转换逻辑\n" +
                "                          ,CASE\n" +
                "                             WHEN t1.c_taflag = '1' THEN\n" +
                "                              '1'\n" +
                "                             ELSE\n" +
                "                              '0'\n" +
                "                           END AS dk_ta_flag\n" +
                "                          ,t1.c_requestno AS requestno\n" +
                "                          ,t1.f_agio AS agio\n" +
                "                          ,nvl(m1.dict_code, concat('~', t1.c_bonustype)) AS dk_bonus_type -- 新增字典转换逻辑\n" +
                "                          ,nvl(m4.dict_code, concat('~', t1.c_exceedflag)) AS dk_exceedflag -- 新增字典转换逻辑\n" +
                "                          ,t1.c_childnetno AS childnetno\n" +
                "                          ,nvl(m5.dict_code, concat('~', t1.c_freezecause)) AS dk_freeze_cause -- 新增字典转换逻辑\n" +
                "                          ,t1.c_freezeenddate AS freeze_end_date\n" +
                "                          ,t1.c_protocolno AS protocolno\n" +
                "                          ,CASE\n" +
                "                             WHEN t1.srcsys = 'ETF' THEN\n" +
                "                              '8'\n" +
                "                             WHEN t1.srcsys = 'LOF' THEN\n" +
                "                              nvl(m6.dict_code, concat('~', t1.c_bourseflag))\n" +
                "                             ELSE\n" +
                "                              '9'\n" +
                "                           END AS dk_bourseflag -- 新增字典转换逻辑--\n" +
                "                          ,t1.f_backfareagio AS back_fare_agio\n" +
                "                          ,NULL AS fare_ratio\n" +
                "                          ,t1.f_profitbalance AS perf_pay\n" +
                "                          ,t1.f_breachfare AS penalty_fare\n" +
                "                          ,NULL AS self_trade_fee\n" +
                "                          ,NULL AS ag_split_ruleno\n" +
                "                          ,NULL AS ip_addr\n" +
                "                          ,NULL AS dk_is_valid\n" +
                "                          ,t1.srcsys AS dk_system_of_sdata\n" +
                "                          ,NULL AS sdata_serialno\n" +
                "                          ,t1.c_memo AS memo\n" +
                "                          ,NULL AS ddvc\n" +
                "                          ," + batchno + " AS batchno\n" +
                "                          ,cast(current_timestamp() as string) AS inserttime\n" +
                "                          ,cast(current_timestamp() as string) AS updatetime\n" +
                "                          ,t1.srcsys AS dk_system_of_upd\n" +
                "                          ,CAST(t1.d_cdate AS INT) AS ta_cfm_date\n" +
                "                          ,t1.c_bourseflag\n" +
                "                          ,t1.c_businflag\n" +
                "                          ,t1.c_outbusinflag\n" +
                "                          ,t1.areacode\n" +
                "                          ,ds.sk_agency AS sk_agency_of_ds_acct\n" +
                "                          ,ds.dk_capitalmode AS capitalmode\n" +
                "                          ,t1.open_date\n" +
                "                          ,t1.bk_region\n" +
                "                          ,t1.c_childnetno\n" +
                "                          ,CASE t1.c_taflag\n" +
                "                             WHEN '1' THEN\n" +
                "                              '1'\n" +
                "                             ELSE\n" +
                "                              '0'\n" +
                "                           END AS taflag\n" +
                "                          ,t1.sk_product_of_tgt\n" +
                "                          ,t1.bk_product_of_tgt\n" +
                "                          ,nvl(m2.dict_code, concat('~', t1.c_othershare)) AS dk_share_type_of_tgt\n" +
                "                          ,NULL AS sk_account_of_fd_tgt\n" +
                "                          ,t1.bk_fundaccount_of_tgt\n" +
                "                          ,t1.tgt_agencyno\n" +
                "                          ,t1.tgt_netno\n" +
                "                          ,t1.c_sharetype\n" +
                "                          ,t1.c_fundcode\n" +
                "                            FROM " +
                " (    SELECT /*+ BROADCAST(t5),BROADCAST(t6),BROADCAST(c1),BROADCAST(m1) */ CAST(0 AS DECIMAL(18, 0)) AS trd_st_trx_serialno\n" +
                "                        ,nvl(f.sk_invpty_of_cust, -1) AS sk_invpty\n" +
                "                        ,NULL AS dk_cust_type\n" +
                "                        ,nvl(f.sk_account_of_fd, -1) AS sk_account\n" +
                "                        ,nvl(tr.sk_tradeacco_reg, -1) AS sk_tradeacco_reg\n" +
                "                        ,nvl(t5.sk_product, -1) AS sk_product\n" +
                "                        ,nvl(m1.dict_code, concat('~', a.c_sharetype)) AS dk_share_type\n" +
                "                        ,nvl(t5.sk_currency, 1) AS sk_currency\n" +
                "                        ,CASE\n" +
                "                           WHEN a.srcsys IN ('BTA', 'YTA', 'LTA') AND\n" +
                "                                a.c_businflag IN ('98', '99') AND\n" +
                "                                nvl(a.c_otheracco, '') = '' THEN\n" +
                "                            98\n" +
                "                           WHEN a.srcsys IN ('BTA', 'YTA', 'LTA') AND\n" +
                "                                a.c_businflag IN ('98', '99') THEN\n" +
                "                            99\n" +
                "                           WHEN a.srcsys IN ('AL0') AND a.c_businflag='98' AND nvl(a.c_otheracco,'')='' THEN\n" +
                "                            99\n" +
                "                           WHEN a.srcsys IN ('AL0') AND a.c_businflag='98' THEN\n" +
                "                            98\n" +
                "                           ELSE nvl(c1.sk_mkt_trade_type, -1)\n" +
                "                         END AS sk_mkt_trade_type --20210125,新增LTA系统;20210331增加AL0逻辑\n" +
                "                        ,a.c_fundacco\n" +
                "                        ,a.c_tradeacco\n" +
                "                        ,a.c_cserialno\n" +
                "                        ,a.tano\n" +
                "                        ,a.c_agencyno\n" +
                "                        ,a.c_netno\n" +
                "                        ,a.d_date\n" +
                "                        ,a.f_balance\n" +
                "                        ,a.f_shares\n" +
                "                        ,a.f_confirmbalance\n" +
                "                        ,a.f_confirmshares\n" +
                "                        ,a.f_interest\n" +
                "                        ,a.f_interesttax\n" +
                "                        ,a.f_totalfare\n" +
                "                        ,a.f_tradefare\n" +
                "                        ,a.f_tafare\n" +
                "                        ,a.f_agencyfare\n" +
                "                        ,a.f_registfare\n" +
                "                        ,a.f_fundfare\n" +
                "                        ,a.f_stamptax\n" +
                "                        ,a.f_backfare\n" +
                "                        ,a.f_backfare4agt\n" +
                "                        ,a.f_backfare4fund\n" +
                "                        ,a.f_otherfare1\n" +
                "                        ,a.f_returnfare\n" +
                "                        ,a.f_interestshare\n" +
                "                        ,a.f_netvalue\n" +
                "                        ,a.c_status\n" +
                "                        ,a.c_cause\n" +
                "                        ,a.c_taflag\n" +
                "                        ,a.c_requestno\n" +
                "                        ,a.f_agio\n" +
                "                        ,a.c_bonustype\n" +
                "                        ,a.c_exceedflag\n" +
                "                        ,a.c_childnetno\n" +
                "                        ,a.c_freezecause\n" +
                "                        ,a.c_freezeenddate\n" +
                "                        ,a.c_protocolno\n" +
                "                        ,a.c_bourseflag\n" +
                "                        ,a.f_backfareagio\n" +
                "                        ,a.f_profitbalance\n" +
                "                        ,a.f_breachfare\n" +
                "                        ,a.c_memo\n" +
                "                        ," + batchno + " AS batchno\n" +
                "                        ,cast(current_timestamp() as string) AS inserttime\n" +
                "                        ,cast(current_timestamp() as string) AS updatetime\n" +
                "                        ,a.d_cdate\n" +
                "                        ,a.c_businflag\n" +
                "                        ,a.c_outbusinflag\n" +
                "                        ,a.areacode\n" +
                "                        ,CAST(nvl(tr.open_date, 19000101) AS DECIMAL(8, 0)) AS open_date\n" +
                "                        ,NULL            AS bk_region\n" +
                "                        ,t6.sk_product   AS sk_product_of_tgt\n" +
                "                        ,a.c_othercode   AS bk_product_of_tgt\n" +
                "                        ,a.c_othershare\n" +
                "                        ,a.c_otheracco   AS bk_fundaccount_of_tgt\n" +
                "                        ,a.c_otheragency AS tgt_agencyno\n" +
                "                        ,a.c_othernetno  AS tgt_netno\n" +
                "                        ,nvl(a.c_sharetype, 'A') AS c_sharetype\n" +
                "                        ,a.c_fundcode\n" +
                "                        ,a.srcsys\n" +
                "                    FROM " + schema_stg + ".ta_tconfirm a --TA交易流水表\n" +
                "                    LEFT JOIN " + schema_ods + ".agrm_fundaccount f --基金账号表\n" +
                "                      ON f.bk_fundaccount = a.c_fundacco\n" +
                "                     AND f.dk_system_of_sdata = (CASE '" + srcsys + "'\n" +
                "                           WHEN 'ANT' THEN\n" +
                "                            'TA0'\n" +
                "                           ELSE\n" +
                "                            '" + srcsys + "'\n" +
                "                         END)\n" +
                "                    LEFT JOIN " + schema_ods + ".agrm_tradeacco_reg tr --交易账号表\n" +
                "                      ON a.c_fundacco = tr.bk_fundaccount\n" +
                "                     AND a.c_tradeacco = tr.bk_tradeaccount\n" +
                "                     AND a.c_agencyno = tr.agencyno\n" +
                "                     AND a.srcsys = tr.dk_system_of_sdata\n" +
                "                     AND tr.dk_system_of_sdata = '" + srcsys + "'\n" +
                "                    LEFT JOIN " + schema_ods + ".prod_product t5 --产品信息表\n" +
                "                      ON t5.bk_product = a.c_fundcode\n" +
                "                    LEFT JOIN " + schema_ods + ".prod_product t6 --产品信息表\n" +
                "                      ON t6.bk_product = a.c_othercode\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_mkt_tradetype_mapp c1 --交易类型映射表\n" +
                "                      ON c1.bk_mkt_trade_type_src = a.c_businflag\n" +
                "                     AND c1.dk_system_of_src = a.srcsys\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m1 --字典映射表获取份额类型\n" +
                "                      ON m1.dict_type = 'DK_SHARE_TYPE'\n" +
                "                     AND m1.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m1.src_dict_code = nvl(a.c_sharetype, 'A')\n" +
                "                   WHERE a.d_cdate --按月分区\n" +
                "                     BETWEEN concat(floor(" + startdate + "/100),'01') and concat(floor(" + enddate + "/100),'31')\n" +
                "                     AND a.srcsys = '" + srcsys + "' --源系统\n" +
                "             ) t1 \n" +
                "                    LEFT JOIN (SELECT bk_tradeaccount\n" +
                "                                     ,agencyno\n" +
                "                                     ,MAX(sk_agency) AS sk_agency\n" +
                "                                     ,MAX(dk_capitalmode) AS dk_capitalmode\n" +
                "                                     ,'" + srcsys + "' AS dk_system_of_sdata\n" +
                "                                 FROM " + schema_ods + ".agrm_tradeaccount_ds --直销交易账号\n" +
                "                                GROUP BY bk_tradeaccount, agencyno" +
                " ) ds\n" +
                "                      ON t1.c_tradeacco = ds.bk_tradeaccount\n" +
                "                     AND t1.c_agencyno = ds.agencyno\n" +
                "                    LEFT JOIN " + schema_ods + ".agrm_saration sa\n" +
                "                      ON sa.sk_product = t1.sk_product\n" +
                "                     AND sa.bk_fundaccount = t1.c_fundacco\n" +
                "                     AND sa.bk_tradeaccount = t1.c_tradeacco\n" +
                "                     AND sa.agencyno = t1.c_agencyno\n" +
                "                     AND sa.protocolno = t1.c_protocolno\n" +
                "                     AND sa.dk_share_type = t1.dk_share_type\n" +
                "                     AND sa.dk_tano = t1.tano\n" +
                "                     AND sa.dk_system_of_sdata = '" + srcsys + "'\n" +
                "                     AND sa.first_date <= CAST(t1.d_cdate AS INT)\n" +
                "                     AND sa.close_date >= CAST(t1.d_cdate AS INT)\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m1 --分红方式字典映射\n" +
                "                      ON m1.dict_type = 'DK_BONUS_TYPE'\n" +
                "                     AND m1.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m1.src_dict_code = t1.c_bonustype\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m2 --份额类型字典映射\n" +
                "                      ON m2.dict_type = 'DK_SHARE_TYPE'\n" +
                "                     AND m2.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m2.src_dict_code = t1.c_othershare\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m3 --失败原因代码字典映射\n" +
                "                      ON m3.dict_type = 'DK_RETCODE'\n" +
                "                     AND m3.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m3.src_dict_code = t1.c_cause\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m4 --巨额赎回处理标志字典映射\n" +
                "                      ON m4.dict_type = 'DK_EXCEEDFLAG'\n" +
                "                     AND m4.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m4.src_dict_code = t1.c_exceedflag\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m5 --冻结原因字典映射\n" +
                "                      ON m5.dict_type = 'DK_FREEZE_CAUSE'\n" +
                "                     AND m5.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m5.src_dict_code = t1.c_freezecause\n" +
                "                    LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m6 --交易来源字典映射\n" +
                "                      ON m6.dict_type = 'DK_BOURSEFLAG'\n" +
                "                     AND m6.dk_system_of_src = '" + srcsys + "'\n" +
                "                     AND m6.src_dict_code = t1.c_bourseflag\n" +
                "                   WHERE t1.srcsys = '" + srcsys + "'\n" +
                "                    -- AND t1.d_cdate BETWEEN '" + startdate + "' AND '" + enddate + "'\n" +
                "                  ) t\n" +
                "        ) t --交易中间表\n" +
                " INNER JOIN " + schema_ods + ".prod_product t1 --产品信息表\n" +
                "    ON t.sk_product=t1.sk_product\n" +
                "  LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m --字典映射表获取份额类别\n" +
                "    ON m.dict_type = 'DK_SHARE_TYPE'\n" +
                "   AND m.dk_system_of_src = '" + srcsys + "'\n" +
                "   AND m.dict_code = t.dk_share_type\n" +
                "  LEFT JOIN " + schema_stg + ".crm_vconfirm_bsjj t2 --CRM交易\n" +
                "    ON t.agencyno  =t2.c_agencyno --销售机构\n" +
                "   AND t.netno=t2.c_netno    --网点号\n" +
                "   AND t.bk_fundaccount  =t2.c_fundacco --基金账号\n" +
                "   AND t.bk_tradeaccount =t2.c_tradeacco --交易账号\n" +
                "   AND t1.BK_PRODUCT =t2.c_FUNDCODE --产品代码\n" +
                "   AND m.src_dict_code =t2.c_sharetype --份额类别\n" +
                "   AND t.cserialno =t2.c_cserialno --确认流水号\n" +
                "   AND t.requestno =t2.c_requestno --申请流水号\n" +
                "   AND t2.d_cdate BETWEEN concat(floor(" + startdate + "/100),'01') and concat(floor(" + enddate + "/100),'31')\n" +
                "   and t2.srcsys = '" + srcsys + "'\n" +
                "  LEFT JOIN " + schema_ods + ".ip_agency t3 --获取销售机构代理键\n" +
                "    ON t2.c_channelno=t3.bk_channelno\n" +
                "  LEFT JOIN " + schema_ods + ".ip_agency t4 --渠道信息表\n" +
                "    ON t.agencyno=t4.agencyno\n" +
                "   and t4.hiear_level=1 --一级\n" +
                " where t.dk_system_of_sdata = '" + srcsys + "'\n";

        String sql2 = "  SELECT /*+ BROADCAST(ag) */ \n" +
                "         reg.sk_tradeacco_reg  --交易账户代理键\n" +
                "        ,reg.sk_account_of_fd  --基金账户代理键\n" +
                "        ,reg.dk_tano  --ta代码\n" +
                "        ,reg.bk_fundaccount  --基金账号\n" +
                "        ,reg.bk_tradeaccount  --交易账号\n" +
                "        ,reg.agencyno  --销售机构代码\n" +
                "        ,reg.netno  --管理网点代码\n" +
                "        ,reg.dk_bonus_type  --分红方式\n" +
                "        ,reg.dk_bankno  --银行账户开户行\n" +
                "        ,reg.bank_account  --银行账户帐号\n" +
                "        ,reg.name_inbank  --银行账户户名\n" +
                "        ,reg.childnetno  --操作网点代码\n" +
                "        ,reg.open_date  --登记日期\n" +
                "        ,reg.close_date  --销户日期\n" +
                "        ,reg.dk_open_flag  --开户网点标志\n" +
                "        ,reg.bk_tradeaccount_of_ag --代销交易账号\n" +
                "        ,reg.sk_agency_of_open --开户销售渠道代理键\n" +
                "        -- Modified By Luogong,20210406：合并更新，关联上的必有值\n" +
                "        ,CASE WHEN tmp.sk_tradeacco_reg IS NULL AND NVL(reg.sk_agency_of_fst, -1) <> -1 THEN reg.sk_agency_of_fst\n" +
                "              WHEN tmp.sk_tradeacco_reg IS NULL AND NVL(reg.sk_agency_of_fst, -1) = -1 THEN ag.sk_agency\n" +
                "              WHEN tmp.sk_tradeacco_reg IS NOT NULL THEN tmp.sk_agency_of_fst\n" +
                "          ELSE reg.sk_agency_of_fst END AS sk_agency_of_fst --若有挂靠到网点，则取新增挂到网点记录，若没有且原有sk不为-1则不更新，否则更新为顶层销售商sk\n" +
                "        ,reg.sk_agency_of_lst             --最近交易销售渠道代理键\n" +
                "        ,reg.dk_org_tree_of_branch    --归属管理区划架构树\n" +
                "        ,reg.sk_invpty_of_org_branch    --归属管理区划\n" +
                "        ,reg.dk_org_tree_of_bl    --归属管理区划架构树-业务条线\n" +
                "        ,reg.sk_invpty_of_org_bl    --归属管理区划-业务条线\n" +
                "        ,reg.dk_is_valid    --是否有效\n" +
                "        ,reg.sdata_serialno    --记录来源唯一标识\n" +
                "        ,reg.memo    --备注\n" +
                "        ,reg.ddvc    --数据校验码\n" +
                "        ,reg.dk_system_of_upd    --记录更新系统\n" +
                "        ,reg.batchno    --批次号\n" +
                "        ,reg.inserttime    --数据插入时间戳\n" +
                "        ,reg.updatetime    --数据更新间戳\n" +
                "        ,reg.dk_account_status    --交易账户状态\n" +
                "        ,reg.dk_system_of_sdata  --源系统\n" +
                "    FROM  tmp.agrm_tradeacco_reg_mth reg\n" +
                "   LEFT JOIN  (SELECT k.sk_tradeacco_reg     --交易账号代理键\n" +
                "                     ,k.sk_account_of_fd   --基金账号代理键\n" +
                "                     ,k.sk_agency_of_fst   --首次交易网点\n" +
                "                     ,k.ta_cfm_date   --确认日期\n" +
                "                     ,k.cserialno   --确认流水号\n" +
                "                     ,CAST(current_timestamp()AS STRING) AS inserttime   --插入时间戳\n" +
                "                     ,CAST(current_timestamp()AS STRING) AS updatetime   --更新时间戳\n" +
                "                     ,k.sk_agency_of_fst_reg   --更新前首次交易网点\n" +
                "                     ,k.dk_top_flag   --更新前是否挂顶层销售机构\n" +
                "                     ,k.dk_system_of_sdata   --源系统\n" +
                "             FROM ( SELECT t.sk_tradeacco_reg ,\n" +
                "                      t.sk_account_of_fd ,\n" +
                "                      t.sk_agency_of_fst ,\n" +
                "                      t.ta_cfm_date ,\n" +
                "                      t.cserialno ,\n" +
                "                      t.sk_agency_of_fst_reg ,\n" +
                "                      t.dk_top_flag ,\n" +
                "                      t.dk_system_of_sdata ,\n" +
                "                      row_number() over(PARTITION BY t.sk_tradeacco_reg, t.sk_account_of_fd\n" +
                "                                        ORDER BY t.ta_cfm_date, t.cserialno ) AS rn \n" +
                "              			 FROM  \n" +
                "                 (SELECT /*+ BROADCAST(ag),BROADCAST(ag2)  */ reg.sk_tradeacco_reg ,\n" +
                "                         reg.sk_account_of_fd ,\n" +
                "                         ag.sk_agency AS sk_agency_of_fst ,\n" +
                "                         trd.ta_cfm_date ,\n" +
                "                         c_cserialno as cserialno ,\n" +
                "                         nvl(reg.sk_agency_of_fst, -1) AS sk_agency_of_fst_reg ,\n" +
                "                         CASE\n" +
                "                             WHEN ag2.sk_agency IS NOT NULL THEN '1'\n" +
                "                             ELSE '0'\n" +
                "                         END AS dk_top_flag , --顶层标记：根据机构代码判断是否挂靠到顶层\n" +
                "                         reg.dk_system_of_sdata\n" +
                "                  FROM\n" +
                "   (   SELECT t.c_fundacco,\n" +
                "                            t.c_tradeacco,\n" +
                "                            t.c_agencyno,\n" +
                "                            t.c_status,\n" +
                "                            c_channelno,\n" +
                "                            cast(d_cdate AS decimal(8,0)) ta_cfm_date,\n" +
                "                            t.c_cserialno,\n" +
                "                            ROW_NUMBER()over(PARTITION BY t.c_fundacco,t.c_tradeacco,t.c_agencyno\n" +
                "                                             ORDER BY d_cdate,c_cserialno) rn--根据交易账号、基金账号、机构代码分区排序，取挂到网点的第一笔交易\n" +
                "                     FROM " + schema_stg + ".crm_vconfirm_bsjj t\n" +
                "                     WHERE t.srcsys ='" + srcsys + "'\n" +
                "                       AND T.d_cdate BETWEEN '" + startdate + "' AND '" + enddate + "'\n" +
                "                       AND c_status='1'  --成功交易\n" +
                "                       AND length(c_channelno)>8 --大于8表示挂靠到网点，不是顶层销售商\n" +
                "                       ) trd\n" +
                "                  INNER JOIN tmp.agrm_tradeacco_reg_mth reg  --关联交易账号表，获取挂到到顶层的记录\n" +
                "                  ON trd.c_fundacco = reg.bk_fundaccount\n" +
                "                  AND trd.c_tradeacco = reg.bk_tradeaccount\n" +
                "                  AND trd.c_agencyno=reg.agencyno\n" +
                "                  AND reg.dk_system_of_sdata = '" + srcsys + "'--20201216,限制分区避免锁表报错\n" +
                "                  LEFT JOIN " + schema_ods + ".ip_agency ag ON trd.c_channelno=ag.bk_channelno --获取网点代理键\n" +
                "                  LEFT JOIN " + schema_ods + ".ip_agency ag2 ON ag2.hiear_level = 1  --判断当前是否挂靠到顶层\n" +
                "                  AND ag2.sk_agency = reg.sk_agency_of_fst\n" +
                "                  WHERE trd.rn=1  --只取第一笔交易\n" +
                "                  ) t\n" +
                "               WHERE (t.sk_agency_of_fst_reg = -1   OR t.dk_top_flag = '1')--限制只取挂到到顶层的记录\n" +
                "                         ) k\n" +
                "                   WHERE k.rn = 1\n" +
                "               ) tmp --获取挂靠到网点的增量记录\n" +
                "      ON reg.sk_tradeacco_reg = tmp.sk_tradeacco_reg\n" +
                "     AND reg.sk_account_of_fd = tmp.sk_account_of_fd\n" +
                "   LEFT JOIN " + schema_ods + ".ip_agency ag  --获取顶层销售机构sk\n" +
                "     ON ag.hiear_level = 1\n" +
                "    AND reg.agencyno = ag.agencyno\n" +
                "   WHERE reg.dk_system_of_sdata = '" + srcsys + "'\n";


        String sql3 = "  SELECT " +
                //" default.genseq('seq_trd_st_trx_serialno','20000','1',t.bk_fundaccount)" +
                " null AS trd_st_trx_serialno,--交易流水号\n" +
                "       cast(nvl(f.sk_invpty_of_cust,-1) as decimal(12,0)) AS sk_invpty_of_cust,--客户参与者代理键\n" +
                "       c.dk_cust_type, --新模型字段--客户类型\n" +
                "       cast(nvl(b.sk_account,-1) as decimal(12,0)) AS sk_account_of_fd,--基金账户代理键\n" +
                "       cast(nvl(tr.sk_tradeacco_reg,-1)  as decimal(12,0)) AS sk_tradeacco_reg,--ta登记交易账户代理键\n" +
                "       cast(if(nvl(tr.sk_agency_of_fst,-1) = -1,t.sk_agency,tr.sk_agency_of_fst) as decimal(12,0))  as sk_agency,--销售渠道代理键\n" +
                "       t.sk_product,--产品代理键\n" +
                "       t.dk_share_type,--收费模式\n" +
                "       t.sk_currency,--结算币种\n" +
                "       t.sk_agreement AS sk_agreement_of_ration,--定投协议代理键\n" +
                "       t.sk_mkt_trade_type,--基金销售交易类型代理键\n" +
                "       t.bk_fundaccount,--基金帐号\n" +
                "       t.bk_tradeaccount,--交易账号\n" +
                "       t.cserialno,--交易确认流水号\n" +
                "       t.dk_tano,--ta代码\n" +
                "       t.agencyno,--销售机构代码\n" +
                "       t.netno,--托管网点号码\n" +
                "       cast(t.transaction_date as decimal(8,0)) as transaction_date, --新模型字段--交易发生日期\n" +
                "       cast(t.request_date as decimal(8,0)) as request_date,--申请日期\n" +
                "       t.req_balance,--申请金额\n" +
                "       t.req_shares,--申请份额\n" +
                "       t.confirm_balance,--确认金额\n" +
                "       t.confirm_shares,--确认份额\n" +
                "       t.interest,--利息\n" +
                "       t.interest_tax,--利息税\n" +
                "       t.total_fare as total_fee,--总费用\n" +
                "       t.trade_fare as trade_fee,--手续费\n" +
                "       t.ta_fare as ta_fee,--过户费\n" +
                "       t.agency_fare as agency_fee,--代销费\n" +
                "       t.regist_fare as regist_fee,--注册登记人费\n" +
                "       t.fund_fare as fund_fee,--归基金资产费\n" +
                "       t.stamp_duty,--印花税\n" +
                "       t.back_fare as back_fee,--后收费用\n" +
                "       t.agency_backfare as agency_backfare,--后收费归销售机构\n" +
                "       t.manager_backfare as manager_backfare,--后收费归管理人\n" +
                "       t.fund_backfare as fund_backfare,--后收费归基金资产\n" +
                "       t.other_fare as other_fee,--其他费用\n" +
                "       t.return_fare as return_fee,--返还保费\n" +
                "       t.interest_share,--认购期利息转份额\n" +
                "       t.net_value,--成交单位净值\n" +
                "       t.dk_saletran_status,--交易处理返回代码\n" +
                "       t.dk_retcode,--失败原因代码\n" +
                "       t.dk_taflag AS dk_ta_flag,--ta发起标志\n" +
                "       t.requestno,--销售商申请编号\n" +
                "       t.agio,--实际折扣率\n" +
                "       t.dk_bonus_type,--分红方式\n" +
                "       t.dk_exceedflag,--巨额赎回处理标志\n" +
                "       t.childnetno,--交易网点号码\n" +
                "       t.dk_freeze_cause,--冻结原因\n" +
                "       cast(t.freeze_end_date as decimal(8,0)) as freeze_end_date ,--冻结截止日期\n" +
                "       t.protocolno,--销售机构定投协议号\n" +
                "       t.dk_bourseflag,--交易来源\n" +
                "       cast(t.back_fare_agio as decimal(6,4)) as back_fare_agio,--后端申购费折扣\n" +
                "       cast(t.fare_ratio as decimal(6,4)) as fare_ratio,--交易费率\n" +
                "       t.perf_pay,--业绩报酬\n" +
                "       t.penalty_fare as penalty_fee,--违约金\n" +
                "       t.self_trade_fee AS trade_fee_income,--交易手续费收入\n" +
                "       null as ag_split_ruleno,--渠道拆分规则序号\n" +
                "       t.ip_addr,--ip地址\n" +
                "		null as sk_product_of_tgt, --对方产品代理键\n" +
                "       null as bk_product_of_tgt, --对方产品代码\n" +
                "       null as dk_share_type_of_tgt, --份额类型\n" +
                "       null as sk_account_of_fd_tgt, --对方基金账户代理键\n" +
                "       null as bk_fundaccount_of_tgt, --对方基金账号\n" +
                "       null as tgt_agencyno, --对方销售机构代码\n" +
                "       null as tgt_netno, --对方管理网点代码\n" +
                "       null as dk_businflag, --TA内部业务代码\n" +
                "       null as dk_outbusinflag, --销售机构业务代码\n" +
                "       t.dk_is_valid,--是否有效1-是，0-否\n" +
                "       t.dk_system_of_upd,--数据更新系统\n" +
                "       t.sdata_serialno,--源记录唯一标识指记录首次进入时源记录标识\n" +
                "       t.memo,--备注\n" +
                "       t.ddvc,--数据校验码\n" +
                "       cast(t.batchno as decimal(16,0)) as batchno,--批次号\n" +
                "       t.inserttime,--数据插入时间戳\n" +
                "       t.updatetime,--更新时间戳\n" +
                "       t.request_time,--申请日期\n" +
                "       cast(t.ta_cfm_date as decimal(8,0)) as ta_cfm_date,--TA确认日期\n" +
                "       t.dk_system_of_sdata,--数据来源系统\n" +
                "       cast(t.ta_cfm_date_m as decimal(6,0)) as ta_cfm_date_m --TA确认月份\n" +
                "  FROM (SELECT /*+ BROADCAST(ia),BROADCAST(p),BROADCAST(m1),BROADCAST(m2),BROADCAST(c1) */ 0 AS trd_st_trx_serialno,\n" +
                "               ia.sk_agency as sk_agency,\n" +
                "               g.sk_product AS sk_product,\n" +
                "               nvl(m1.dict_code, concat('~',a.c_sharetype)) AS dk_share_type,\n" +
                "               p.sk_currency AS sk_currency,\n" +
                "               cast(-1 as decimal(12,0)) AS sk_agreement,\n" +
                "               nvl(c1.sk_mkt_trade_type, -1) AS sk_mkt_trade_type,\n" +
                "               a.c_fundacco AS bk_fundaccount,\n" +
                "               a.c_tradeacco AS bk_tradeaccount,\n" +
                "               a.c_cserialno AS cserialno,\n" +
                "               a.tano AS dk_tano,\n" +
                "               a.c_agencyno AS agencyno,\n" +
                "               nvl(a.c_netno,a.c_agencyno) AS netno,\n" +
                "               NULL AS transaction_date, --新模型表字段\n" +
                "               cast(a.d_regdate AS INT) AS request_date,\n" +
                "               cast(0 as decimal(21,6)) AS req_balance,\n" +
                "               cast(0 as decimal(17,2)) AS req_shares,\n" +
                "               CASE a.c_flag\n" +
                "                 WHEN '0' THEN\n" +
                "                   a.f_reinvestbalance\n" +
                "                 WHEN '1' THEN\n" +
                "                   a.f_realbalance\n" +
                "                 ELSE\n" +
                "                   a.f_realbalance\n" +
                "               END AS confirm_balance,\n" +
                "               a.f_realshares AS confirm_shares,\n" +
                "               cast(0 as decimal(30,6)) AS interest,\n" +
                "               cast(0 as decimal(21,6)) AS interest_tax,\n" +
                "               a.f_fare AS total_fare,\n" +
                "               cast(0 as decimal(21,6)) AS trade_fare,\n" +
                "               cast(0 as decimal(21,6)) AS ta_fare,\n" +
                "               cast(0 as decimal(21,6)) AS agency_fare,\n" +
                "               cast(0 as decimal(21,6)) AS regist_fare,\n" +
                "               cast(0 as decimal(21,6)) AS fund_fare,\n" +
                "               cast(0 as decimal(21,6)) AS stamp_duty,\n" +
                "               cast(0 as decimal(21,6)) AS back_fare,\n" +
                "               cast(0 as decimal(21,6)) AS agency_backfare,\n" +
                "               cast(0 as decimal(21,6)) AS manager_backfare,\n" +
                "               cast(0 as decimal(21,6)) AS fund_backfare,\n" +
                "               cast(0 as decimal(21,6)) AS other_fare,\n" +
                "               cast(0 as decimal(21,6)) AS return_fare,\n" +
                "               cast(0 as decimal(17,2)) AS interest_share,\n" +
                "               a.f_netvalue AS net_value,\n" +
                "               '1' AS dk_saletran_status,\n" +
                "               '0000' AS dk_retcode,\n" +
                "               '1' AS dk_taflag,\n" +
                "               NULL AS requestno,\n" +
                "               NULL AS agio,\n" +
                "               nvl(m2.dict_code, concat('~',a.c_flag)) AS dk_bonus_type,\n" +
                "               NULL AS dk_exceedflag,\n" +
                "               NULL AS childnetno,\n" +
                "               NULL AS dk_freeze_cause,\n" +
                "               NULL AS freeze_end_date,\n" +
                "               NULL AS protocolno,\n" +
                "               '9' AS dk_bourseflag,\n" +
                "               cast(0 as decimal(6,4)) AS back_fare_agio,\n" +
                "               cast(0 as decimal(6,4)) AS fare_ratio,\n" +
                "               cast(0 as decimal(21,6)) AS perf_pay,\n" +
                "               cast(0 as decimal(21,6)) AS penalty_fare,\n" +
                "               cast(0 as decimal(21,6)) AS self_trade_fee,\n" +
                "               null AS ag_split_ruleno,\n" +
                "               NULL AS ip_addr, -- 新模型表字段\n" +
                "               NULL AS dk_is_valid, --  新模型表字段\n" +
                "               a.srcsys AS dk_system_of_sdata, --新模型表字段\n" +
                "               '0' AS sdata_serialno, --  新模型表字段\n" +
                "               '分红数据补充' AS memo, --新模型表字段\n" +
                "               NULL AS ddvc, --新模型表字段\n" +
                "               " + batchno + " AS batchno,\n" +
                "               cast(current_timestamp() as string) AS inserttime,\n" +
                "               cast(current_timestamp() as string) AS updatetime,\n" +
                "               null as request_time,\n" +
                "               a.d_cdate as ta_cfm_date,\n" +
                "               '" + srcsys + "' as dk_system_of_upd,\n" +
                "               cast(substr(a.d_cdate,1,6) AS INT) as ta_cfm_date_m\n" +
                "          FROM " + schema_stg + ".ta_tdividenddetail a --分红流水表\n" +
                "          LEFT JOIN " + schema_ods + ".IP_AGENCY IA --从IP_AGENCY取顶层销售机构sk\n" +
                "           ON IA.agencyno=A.c_agencyno\n" +
                "          AND IA.hiear_level=1\n" +
                "          LEFT JOIN(SELECT /*+ BROADCAST(t2) */ DISTINCT\n" +
                "                           t2.sk_product,--产品代理建\n" +
                "                           t2.bk_product_src,--源产品代码\n" +
                "                           t2.dk_system_of_src,--源系统\n" +
                "                           t1.d_cdate --确认日期\n" +
                "                      FROM " + schema_stg + ".ta_tdividenddetail t1 --分红流水表\n" +
                "                      JOIN " + schema_ods + ".prod_product_mapp t2 --产品MAPP表\n" +
                "                        ON t2.bk_product_src   = t1.c_fundcode\n" +
                "                       AND t2.dk_system_of_src = t1.srcsys\n" +
                "                     WHERE CAST(t1.d_cdate AS INT) BETWEEN t2.effective_from AND t2.effective_to\n" +
                "                       and t1.srcsys='" + srcsys + "'--源系统\n" +
                "                       and t1.d_cdate --按月分区\n" +
                "                       BETWEEN concat(floor(" + startdate + "/100),'01') and concat(floor(" + enddate + "/100),'31')\n" +
                "                     -- substr(t1.d_cdate,1,6) substr('" + startdate + "',1,6) AND substr('" + enddate + "',1,6)\n" +
                "                       ) g\n" +
                "            ON g.bk_product_src = a.c_fundcode\n" +
                "           AND g.dk_system_of_src = a.srcsys\n" +
                "           and a.d_cdate = g.d_cdate\n" +
                "          LEFT JOIN " + schema_ods + ".prod_product p --产品信息表\n" +
                "            ON g.sk_product = p.sk_product\n" +
                "          LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m1 --关联字典映射表获取份额类别\n" +
                "            ON m1.dict_type = 'DK_SHARE_TYPE'\n" +
                "           AND m1.dk_system_of_src = a.srcsys\n" +
                "           AND m1.src_dict_code = nvl(a.c_sharetype,'A')\n" +
                "           and m1.dk_system_of_src = '" + srcsys + "'\n" +
                "          LEFT JOIN " + schema_ods + ".comm_src2odsdict_mapp m2 --关联字典映射表获取分红方式\n" +
                "            ON m2.dict_type = 'DK_BONUS_TYPE'\n" +
                "           AND m2.dk_system_of_src = a.srcsys\n" +
                "           AND m2.src_dict_code = a.c_flag\n" +
                "           and m2.dk_system_of_src = '" + srcsys + "'\n" +
                "          LEFT JOIN " + schema_ods + ".comm_mkt_tradetype_mapp c1 --交易类型字典映射表\n" +
                "            ON c1.bk_mkt_trade_type_src='143'  --143分红\n" +
                "           AND c1.dk_system_of_src = a.srcsys\n" +
                "         WHERE a.d_cdate\n" +
                "               BETWEEN concat(floor(" + startdate + "/100),'01') and concat(floor(" + enddate + "/100),'31')\n" +
                "           AND a.srcsys = '" + srcsys + "') t\n" +
                "  LEFT JOIN " + schema_ods + ".agrm_account_mapp b --账户MAPP表\n" +
                "    on t.bk_fundaccount = b.bk_account_src\n" +
                "   and t.dk_system_of_sdata = b.dk_system_of_src\n" +
                "   and b.dk_src_account_category = 'TA'\n" +
                "   AND b.dk_system_of_src = '" + srcsys + "'\n" +
                "  LEFT JOIN " + schema_ods + ".agrm_fundaccount f --基金账号表\n" +
                "    ON b.sk_account = f.sk_account_of_fd\n" +
                "  LEFT JOIN " + schema_ods + ".ip_cust c\n" +
                "    ON f.sk_invpty_of_cust = c.sk_invpty_of_cust\n" +
                "  LEFT JOIN tmp.agrm_tradeacco_reg_mth tr --交易账号表\n" +
                "    ON t.bk_fundaccount = tr.bk_fundaccount\n" +
                "   AND t.bk_tradeaccount = tr.bk_tradeaccount\n" +
                "   AND t.agencyno = tr.agencyno\n" +
                "   AND t.dk_system_of_sdata = tr.dk_system_of_sdata\n" +
                "   and tr.dk_system_of_sdata = '" + srcsys + "' ";


        System.out.println(sql_lookdf1);
        System.out.println();
        System.out.println(sql_lookdf2);
        System.out.println();

        System.out.println(sql1);
        System.out.println();
        System.out.println(sql2);
        System.out.println();
        System.out.println(sql3);
        System.out.println();

        Dataset<Row> lookdf1 = hc.sql(sql_lookdf1);
        Dataset<Row> adf = hc.sql(sql1);
        hc.createDataFrame(adf.javaRDD(), lookdf1.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata", "ta_cfm_date_m")
                .mode(SaveMode.Append)
                .saveAsTable("tmp." + sparktable1);

        Dataset<Row> lookdf2 = hc.sql(sql_lookdf2);
        adf = hc.sql(sql2);
        hc.createDataFrame(adf.javaRDD(), lookdf2.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata").mode(SaveMode.Overwrite)
                .saveAsTable("tmp." + sparktable2);

        adf = hc.sql(sql3);
        hc.createDataFrame(adf.javaRDD(), lookdf1.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata", "ta_cfm_date_m")
                .mode(SaveMode.Append)
                .saveAsTable("tmp." + sparktable1);

        hc.close();

    }

}
