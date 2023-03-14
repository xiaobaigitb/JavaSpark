package cn.com.businessmatrix.hive;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.com.businessmatrix.JavaPairFlatMapPartitionFunc;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/*
 *  盈亏-平均成本法
 *  数据要求：
 *  1.双向交易必须是对称的，比如127 vs 128，兴全发现有127 vs 145 。
 *  2.一笔126，不能是多笔127和多笔128，即不能进行分解。
 *    比如转托管5000，分成2000+3000，因为是用申请单号来关联。
 *    如果是多笔，申请单号是同一个，这样一关联就重复了。
 *  3.转托管（包括非交易过户）时，对应的分红也会带走。即分红计入最终受益要求份额结束生命周期即赎回。
 *    带走的分红是用divid_income_out字段实现。
 *  4.补差费用计入转入产品对应份额的成本，但是补差费记录在转出方那一笔上，通过申请单号来过渡。
 *  
 *  实现过程：
 *  步骤一：因为转托管入的成本来源于出的那一刻的平均成本，所以先处理"入"之外的账户交易。
 *  1.1找出做过托管入（内部转托管入、非交易过户入等）交易的账户。
 *  1.2找出没有做过第1步骤交易的账户。
 *  1.3从落地表（ass_ta_cust_income_detail2,ass_ta_cust_income_detail_et2)里面取出最后一条记录(99991231),
 *     关联1.2的临时表实现增量处理，即只是处理有交易的。
 *     关联1.1的临时表实现先处理出的账户，入的账户放到下面的步骤二。
 *  1.4找出138（基金转换出）的交易，如果没有跨TA的基金转换，就可以进一步过滤搜索的范围。
 *  1.5从交易表取出交易。
 *     关联1.2的临时表实现增量处理，即只是处理有交易的。
 *     关联1.1的临时表实现先处理出的交易，入的交易放到下面的步骤二。
 *     关联1.4的临时表实现补差费的处理。
 *  1.6上面的数据送入spark进行处理到临时表。
 *  1.7把spark的数据合并到ass_ta_cust_income_detail2,ass_ta_cust_income_detail_et2。
 *     此时出的交易都处理完成。
 *     
 *  步骤二：因为转托管出的平均成本已经计算好，开始处理"入"的账户交易。
 *  2.1找出做过托管入（内部转托管入、非交易过户入等）交易的账户。 
 *  2.2找出做过托管出（内部转托管出、非交易过户出等）交易时刻的平均成本。
 *  2.3从落地表（ass_ta_cust_income_detail2,ass_ta_cust_income_detail_et2)里面取出最后一条记录(99991231),
 *     关联2.1的临时表实现增量处理，即只是处理有交易的。
 *  2.4找出138（基金转换出）的交易，如果没有跨TA的基金转换，就可以进一步过滤搜索的范围。
 *  2.5从交易表取出交易。
 *     关联2.1的临时表实现增量处理，即只是处理有交易的。
 *     关联2.2的临时表实现得到入的平均成本。
 *     关联2.4的临时表实现补差费的处理。
 *  2.6上面的数据送入spark进行处理到临时表。
 *  2.7把spark的数据合并到ass_ta_cust_income_detail2,ass_ta_cust_income_detail_et2。
 *     此时"入"的交易都处理完成。
 *  2.8稽核数据。  
 *     
 *  参数顺序：
 *   1：	srcsys=args[0];//系统
	 2：	sparktable=args[1];//存放spark临时数据的表
     3： startdate=args[2];//开始日期
	 4： enddate=args[3];//结束日期	
	 5： schema=args[4];//schema
	 6： sparkname=args[5];//spark任务名称，方便查找
	 7： step=args[6];//对应的步骤。
 *  
 *  
 */
public class SparkPLAvgHive2 {
    public static final double err = 0.000001d;
    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {


        String srcsys = args[0];//系统
        String sparktable = args[1];//存放spark临时数据的表
        String startdate = args[2];
        String enddate = args[3];
        String schema = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String step = args[6];

        String transferInTypes = "(127,198,10134)";//args[7];//转托管入，非交易过户入
        String transferOutTypes = "(128,199,10135)";//args[8];//转托管出，非交易过户出
        List<String> transferTypesList = new ArrayList(Arrays.asList(transferInTypes.replace("(", "").replace(")", "").split(",")));
        transferTypesList.addAll(Arrays.asList(transferOutTypes.replace("(", "").replace(")", "").split(",")));
		 /*
		
		String sparkname="ass_ta_cust_income_detail2_88888888";//spark任务名称，方便查找
		String step="2";//第1步骤，先处理出的，第2步骤，再处理入，因为入的成本要等出的成本		
		String transferInTypes="(127,198,10134)";//args[7];//转托管入，非交易过户入		
		String transferOutTypes="(128,199,10135)";//args[8];//转托管出，非交易过户出
		String srcsys="LOFTA";
		String batchno="1";
		String startdate="20040101";
		String   enddate="20150131";
		String schema="ods";
		String sparktable="tmp_ass_ta_cust_income_detail2_spark";
		List<String> transferTypesList= new ArrayList(Arrays.asList(transferInTypes.replace("(", "").replace(")","").split(",")));
        transferTypesList.addAll(Arrays.asList(transferOutTypes.replace("(", "").replace(")","").split(","))); 
		*/


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

        {

            String sql_lookdf = null;
            if ("1".equals(step)) {

                sql_lookdf = "  select cast(("
                        + "concat_ws(',',"
                        + "cast(t1.sk_account_of_fd as string),"
                        + "cast(t1.sk_tradeacco_reg as string),"
                        + "cast(t1.sk_product       as string),"
                        + "t1.dk_share_type)) as string)  as hashcode, " + //0
                        "       t1.trd_it_trx_serialno," + //1
                        "       t1.dk_tano," + //2
                        "       t1.cserialno, " + //3
                        "       t1.requestno, " +  //4
                        "       t1.sk_invpty_of_cust, " +//5
                        "       t1.dk_cust_type, " + //6
                        "       t1.sk_account_of_fd, " +//7
                        "       t1.sk_tradeacco_reg, " + //8
                        "       t1.sk_currency, " +  //9
                        "       t1.agencyno, " + //10
                        "       t1.netno, " + //11
                        "       t1.sk_product, " + //12
                        "       t1.dk_share_type, " + //13
                        "       t1.sk_agency, " + //14
                        "       t2.effective_to, " + //15
                        "       t1.net_value, " + //16
                        "       t1.sk_mkt_trade_type, " +//17
                        "       t1.dk_bourseflag, " + //18
                        "       t1.share_change, " + //19
                        "       t1.shares, " + //20
                        "       t1.avg_cost, " + //21
                        "       t1.divid_income, " + //22
                        "       t1.divid_income_out, " + //23
                        "       t1.total_income, " + //24
                        "       t1.total_fee, " + //25
                        "       t1.dk_is_valid, " + //26
                        "       t1.sdata_serialno, " + //27
                        "       t1.memo, " + //28
                        "       t1.ddvc, " + //29
                        "       t1.batchno, " + //30
                        "       t1.inserttime, " + //31
                        "       t1.updatetime, " + //32
                        "       t1.dk_system_of_upd, " +//33
                        "       t1.effective_from, " + //34
                        "       t1.dk_system_of_sdata " + //35
                        "  from " + schema + ".ass_ta_cust_income_detail2 t1 inner join " +
                        schema + ".ass_ta_cust_income_detail_et2 t2 on t1.trd_it_trx_serialno=t2.trd_it_trx_serialno " +
                        " inner join  " + schema + ".tmp_ass_ta_cust_income_detail2_notin t  on t1.sk_account_of_fd = t.sk_account_of_fd " +
                        " and t1.sk_tradeacco_reg = t.sk_tradeacco_reg " +
                        " and t1.sk_product       = t.sk_product " +
                        " and t1.dk_share_type    = t.dk_share_type " +
                        " left join   " + schema + ".tmp_ass_ta_cust_income_detail2_in t3 on  t1.sk_account_of_fd = t3.sk_account_of_fd " +
                        " and t1.sk_tradeacco_reg = t3.sk_tradeacco_reg " +
                        " and t1.sk_product 	  = t3.sk_product " +
                        " and t1.dk_share_type    = t3.dk_share_type " +
                        " where t1.dk_system_of_sdata='" + srcsys + "'  and t2.effective_to=99991231 " +
                        " and t1.effective_from<=" + enddate +  //过滤掉不能参加扣减的数据
                        " and t3.sk_account_of_fd is null " +
                        //" and t1.sk_account_of_fd in(96977247) " +
                        " "
                ;
            } else {
                sql_lookdf = "   select cast(("
                        + "concat_ws(',',"
                        + "cast(t1.sk_account_of_fd as string),"
                        + "cast(t1.sk_tradeacco_reg as string),"
                        + "cast(t1.sk_product       as string),"
                        + "t1.dk_share_type)) as string)  as hashcode, " + //0
                        "       t1.trd_it_trx_serialno," + //1
                        "       t1.dk_tano," + //2
                        "       t1.cserialno, " + //3
                        "       t1.requestno, " +  //4
                        "       t1.sk_invpty_of_cust, " +//5
                        "       t1.dk_cust_type, " + //6
                        "       t1.sk_account_of_fd, " +//7
                        "       t1.sk_tradeacco_reg, " + //8
                        "       t1.sk_currency, " +  //9
                        "       t1.agencyno, " + //10
                        "       t1.netno, " + //11
                        "       t1.sk_product, " + //12
                        "       t1.dk_share_type, " + //13
                        "       t1.sk_agency, " + //14
                        "       t2.effective_to, " + //15
                        "       t1.net_value, " + //16
                        "       t1.sk_mkt_trade_type, " +//17
                        "       t1.dk_bourseflag, " + //18
                        "       t1.share_change, " + //19
                        "       t1.shares, " + //20
                        "       t1.avg_cost, " + //21
                        "       t1.divid_income, " + //22
                        "       t1.divid_income_out, " + //23
                        "       t1.total_income, " + //24
                        "       t1.total_fee, " + //25
                        "       t1.dk_is_valid, " + //26
                        "       t1.sdata_serialno, " + //27
                        "       t1.memo, " + //28
                        "       t1.ddvc, " + //29
                        "       t1.batchno, " + //30
                        "       t1.inserttime, " + //31
                        "       t1.updatetime, " + //32
                        "       t1.dk_system_of_upd, " +//33
                        "       t1.effective_from, " + //34
                        "       t1.dk_system_of_sdata " + //35
                        "  from " + schema + ".ass_ta_cust_income_detail2 t1 inner join " +
                        schema + ".ass_ta_cust_income_detail_et2 t2 on t1.trd_it_trx_serialno=t2.trd_it_trx_serialno " +
                        " inner join   " + schema + ".tmp_ass_ta_cust_income_detail2_in t3 on  t1.sk_account_of_fd = t3.sk_account_of_fd " +
                        " and t1.sk_tradeacco_reg = t3.sk_tradeacco_reg " +
                        " and t1.sk_product 	  = t3.sk_product " +
                        " and t1.dk_share_type    = t3.dk_share_type " +
                        " where t1.dk_system_of_sdata='" + srcsys + "'  and t2.effective_to=99991231 " +
                        " and t1.effective_from<=" + enddate +  //过滤掉不能参加扣减的数据
                        //" and t1.sk_account_of_fd in(96977247) " +
                        "  "
                ;
            }

            System.out.println(sql_lookdf);
            System.out.println();


            String sql = null;
            if ("1".equals(step)) {
                sql = "  select cast(("
                        + "concat_ws(',',"
                        + "cast(t.sk_account_of_fd as string),"
                        + "cast(nvl(tap.sk_tradeacco_reg_of_in,t.sk_tradeacco_reg) as string),"
                        + "cast(t.sk_product       as string),"
                        + "t.dk_share_type)) as string) as hashcode " +
                        "               ,t.sk_invpty_of_cust " +
                        "               ,t.dk_cust_type " +
                        "               ,t.sk_account_of_fd " +
                        "               ,nvl(tap.sk_tradeacco_reg_of_in,t.sk_tradeacco_reg) as sk_tradeacco_reg " +
                        "               ,t.sk_agency " +
                        "               ,t.sk_product " +
                        "               ,t.dk_share_type " +
                        "               ,t.sk_currency " +
                        "               ,t.sk_mkt_trade_type " +
                        "               ,t.cserialno " +
                        "               ,t.dk_tano " +
                        "               ,t.agencyno " +
                        "               ,t.netno " +
                        "               ,nvl(t.confirm_balance,t.confirm_shares*nvl(nvl(t.net_value,pn.net_value),1.0)) as confirm_balance " +
                        "               ,t.confirm_shares " +
                        "               ,nvl(t3.back_fee,0) +nvl(t.total_fee,0)-nvl(t.back_fee,0) as total_fee " + //补差费从转出方转移到转入方
                        "               ,nvl(nvl(t.net_value,pn.net_value),1.0) as net_value " +
                        "               ,t.requestno " +
                        "               ,t.dk_bonus_type " +
                        "               ,t.dk_bourseflag" +
                        "               ,t.dk_system_of_sdata " +
                        "               ,t.ta_cfm_date " +
                        "               ,m.shr_chg_dir " +
                        "               ,m.income_rule " +
                        "               ,t.batchno " +
                        "               ,0.0 as avg_cost " +
                        "               ,0.0 as divid_income_out " +
                        "  from " + schema + ".trd_ta_saletran t " +
                        " left join  " + schema + ".comm_mkt_tradetype m on t.sk_mkt_trade_type=m.sk_mkt_trade_type " +
                        " left join  " + schema + ".prod_product pp on t.sk_product=pp.sk_product " +
                        " left join  " + schema + ".agrm_tradeaccomap tap on t.sk_tradeacco_reg=tap.sk_tradeacco_reg_of_out and t.dk_tano=tap.dk_tano " +
                        " inner join   " + schema + ".tmp_ass_ta_cust_income_detail2_notin t1 on t1.sk_account_of_fd = t.sk_account_of_fd " +
                        " and t1.sk_tradeacco_reg = t.sk_tradeacco_reg " +
                        " and t1.sk_product       = t.sk_product " +
                        " and t1.dk_share_type    = t.dk_share_type " +
                        " left join   " + schema + ".tmp_ass_ta_cust_income_detail2_in t2 on  t2.sk_account_of_fd = t.sk_account_of_fd " +
                        " and t2.sk_tradeacco_reg = t.sk_tradeacco_reg " +
                        " and t2.sk_product       = t.sk_product " +
                        " and t2.dk_share_type    = t.dk_share_type " +
                        " left join   " + schema + ".tmp_ass_ta_cust_income_detail2_change t3 "
                        + " on t.ta_cfm_date=t3.ta_cfm_date and t.requestno=t3.requestno and t.agencyno=t3.agencyno  " +
                        " left join  " + schema + ".prod_nav pn on t.sk_product=pn.sk_product and t.ta_cfm_date=pn.ta_cfm_date " +
                        " where t.ta_cfm_date between " + startdate + " and " + enddate +
                        " and t.ta_cfm_date_m between floor(" + startdate + "/100) and floor(" + enddate + "/100) " +
                        " and t.dk_system_of_sdata='" + srcsys + "' " +
                        " and pp.dk_is_money_fund='0' and t.dk_saletran_status='1' " +
                        " and (t.confirm_balance>0 or t.confirm_shares>0) and t.sk_mkt_trade_type<>120 " +
                        " and t2.sk_account_of_fd is null " +
                        " and nvl(t3.sk_mkt_trade_type,-1)<>t.sk_mkt_trade_type " + //hive left join 不支持<>连接符号
                        //" and t.sk_account_of_fd in(96977247) " +
                        " "
                ;
            } else {
                sql = "  select cast(("
                        + "concat_ws(',',"
                        + "cast(t.sk_account_of_fd as string),"
                        + "cast(nvl(tap.sk_tradeacco_reg_of_in,t.sk_tradeacco_reg) as string),"
                        + "cast(t.sk_product       as string),"
                        + "t.dk_share_type)) as string) as hashcode " +
                        "               ,t.sk_invpty_of_cust " +
                        "               ,t.dk_cust_type " +
                        "               ,t.sk_account_of_fd " +
                        "               ,nvl(tap.sk_tradeacco_reg_of_in,t.sk_tradeacco_reg) as sk_tradeacco_reg " +
                        "               ,t.sk_agency " +
                        "               ,t.sk_product " +
                        "               ,t.dk_share_type " +
                        "               ,t.sk_currency " +
                        "               ,t.sk_mkt_trade_type " +
                        "               ,t.cserialno " +
                        "               ,t.dk_tano " +
                        "               ,t.agencyno " +
                        "               ,t.netno " +
                        "               ,nvl(t.confirm_balance,t.confirm_shares*nvl(nvl(t.net_value,pn.net_value),1.0)) as confirm_balance " +
                        "               ,t.confirm_shares " +
                        "               ,nvl(t3.back_fee,0) +nvl(t.total_fee,0)-nvl(t.back_fee,0) as total_fee " + //补差费从转出方转移到转入方
                        "               ,nvl(nvl(t.net_value,pn.net_value),1.0) as net_value " +
                        "               ,t.requestno " +
                        "               ,t.dk_bonus_type " +
                        "               ,t.dk_bourseflag" +
                        "               ,t.dk_system_of_sdata " +
                        "               ,t.ta_cfm_date " +
                        "               ,m.shr_chg_dir " +
                        "               ,m.income_rule " +
                        "               ,t.batchno " +
                        "               ,nvl(t2.avg_cost,0) as avg_cost " +
                        "               ,nvl(t2.divid_income_out,0) as divid_income_out " +
                        "  from " + schema + ".trd_ta_saletran t " +
                        " left join  " + schema + ".comm_mkt_tradetype m on t.sk_mkt_trade_type=m.sk_mkt_trade_type " +
                        " left join  " + schema + ".prod_product pp on t.sk_product=pp.sk_product " +
                        " left join  " + schema + ".agrm_tradeaccomap tap on t.sk_tradeacco_reg=tap.sk_tradeacco_reg_of_out and t.dk_tano=tap.dk_tano " +
                        " inner join   " + schema + ".tmp_ass_ta_cust_income_detail2_in t1 on t1.sk_account_of_fd = t.sk_account_of_fd " +
                        " and t1.sk_tradeacco_reg = t.sk_tradeacco_reg " +
                        " and t1.sk_product       = t.sk_product " +
                        " and t1.dk_share_type    = t.dk_share_type " +
                        " left join    " + schema + ".tmp_ass_ta_cust_income_detail2_out  t2 on  t2.sk_product       = t.sk_product " +
                        " and t2.dk_share_type    = t.dk_share_type " +
                        " and t2.requestno        = t.requestno " +
                        " and t2.effective_from   = t.ta_cfm_date " +
                        " left join    " + schema + ".tmp_ass_ta_cust_income_detail2_change  t3 "
                        + " on t.ta_cfm_date=t3.ta_cfm_date and t.requestno=t3.requestno and t.agencyno=t3.agencyno " +
                        " left join  " + schema + ".prod_nav pn on t.sk_product=pn.sk_product and t.ta_cfm_date=pn.ta_cfm_date " +
                        " where t.ta_cfm_date between " + startdate + " and " + enddate +
                        " and t.ta_cfm_date_m between floor(" + startdate + "/100) and floor(" + enddate + "/100) " +
                        " and t.dk_system_of_sdata='" + srcsys + "' " +
                        " and pp.dk_is_money_fund='0' and t.dk_saletran_status='1' " +
                        " and (t.confirm_balance>0 or t.confirm_shares>0)  and t.sk_mkt_trade_type<>120 " +
                        " and nvl(t3.sk_mkt_trade_type,-1)<>t.sk_mkt_trade_type " +
                        //" and t.sk_account_of_fd in(96977247) " +
                        " "
                ;

            }
            System.out.println("sql=");
            System.out.println(sql);
            System.out.println();

            Dataset<Row> lookdf = hc.sql(sql_lookdf);
            lookdf = lookdf.repartition(lookdf.col("hashcode"));

            Dataset<Row> adf = hc.sql(sql);
            adf = adf.repartition(adf.col("hashcode"));

            StructType st = lookdf.schema();
            JavaRDD<Iterable<Row>> w = handleNext2(adf, lookdf, st, transferTypesList);
            JavaRDD<Row> x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
                private static final long serialVersionUID = -6111647437563220114L;

                @Override
                public java.util.Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                    return arg0.iterator();
                }

            });

            //List<Row> lsr=x.collect();
            //System.out.println(lsr);
            System.out.println("===================================================================");
            System.out.println(schema + "." + sparktable);
            hc.createDataFrame(x, lookdf.schema()).write()
                    .format("parquet")
                    .partitionBy("dk_system_of_sdata")
                    .mode(SaveMode.Append)
                    .saveAsTable(schema + "." + sparktable);
        } //end while

        //Thread.currentThread().sleep(1000*1000L);
        hc.close();

    }


    static JavaRDD<Iterable<Row>> handleNext2(Dataset<Row> adf, final Dataset<Row> lookdf, StructType st, final List<String> transferTypesList) {

        JavaPairRDD<Object, Row> ardd = adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();
        JavaPairRDD<Object, Row> lkprdd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> result = ardd.cogroup(lkprdd);

        JavaRDD<Iterable<Row>> w = result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {
            //List<Row> ls=new ArrayList<Row>();  //此变量放置的位置非常重要
            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                List<Row> ls = new ArrayList<Row>();
                Iterable<Row> tr = tuple._2._1;
                Iterable<Row> vr = tuple._2._2;

                java.util.Iterator<Row> itr = tr.iterator();
                java.util.Iterator<Row> ivr = vr.iterator();

                @SuppressWarnings("unchecked")
                List<Row> ltr = IteratorUtils.toList(itr);
                Collections.sort(ltr, new Comparator<Row>() {
                    public int compare(Row r1, Row r2) {
                        //按照日期排，分红排最前面,先份额减少，再份额增加，
                        int comp1 = ((BigDecimal) r1.getAs("ta_cfm_date")).compareTo((BigDecimal) r2.getAs("ta_cfm_date"));
                        int comp2 = 0;
                        int s1 = ((BigDecimal) r1.getAs("sk_mkt_trade_type")).intValue();
                        int s2 = ((BigDecimal) r2.getAs("sk_mkt_trade_type")).intValue();

                        //分红排前面，可能同一天分红和赎回
                        if (s1 == 143 && s2 != 143)
                            comp2 = -1;
                        else if (s1 != 143 && s2 == 143)
                            comp2 = 1;
                        else comp2 = 0;

                        int comp3 = -((BigDecimal) r1.getAs("shr_chg_dir")).compareTo((BigDecimal) r2.getAs("shr_chg_dir"));
                        String c1 = (String) r1.getAs("cserialno");
                        String c2 = (String) r2.getAs("cserialno");
                        int comp4 = c1.compareTo(c2);
                        int comp5 = -2;//按照流水号排序，但是数字格式来排序
                        if (StringUtils.isNumeric(c1) && StringUtils.isNumeric(c2)) {
                            comp5 = new BigDecimal(c1).compareTo(new BigDecimal(c2));
                        }
                        if (comp1 != 0)
                            return comp1;

                        else if (comp2 != 0)
                            return comp2;
                        else if (comp3 != 0)
                            return comp3;
                        else {
                            if (comp5 != -2)
                                return comp5;
                            return comp4;
                        }
                    }
                });

                //排序之后重新取iterator
                itr = ltr.iterator();

                Row tranRow = null;
                Row currRow = null;
                if (ivr.hasNext()) {
                    currRow = ivr.next();//基于前面的保有开始计算
                }

                double[] d = new double[5];
                while (itr.hasNext()) {
                    tranRow = itr.next();
                    if (currRow == null) {
                        //构造第一条出来
                        currRow = changeEt2(tranRow, st, d, transferTypesList);
                    } else {
                        currRow = changeEt(currRow, tranRow, st, d);
                        ls.add(currRow);
                        currRow = changeEt2(tranRow, st, d, transferTypesList);
                    }
                }
                ls.add(currRow);
                return ls;
            }
        });

        return w.filter(new Function<Iterable<Row>, Boolean>() {
            private static final long serialVersionUID = -430944040308979866L;

            @Override
            public Boolean call(Iterable<Row> arg0) throws Exception {
                if (arg0.iterator().hasNext())
                    return true;
                else
                    return false;
            }
        });
    }


    //只是截断effective_to和更新updatetime，老的那条已经在表里面，
    static Row changeEt(Row currRow, Row tranRow, StructType st, double[] params) {
        params[0] = ((BigDecimal) currRow.getAs("shares")).doubleValue();
        if (currRow.getAs("avg_cost") == null)
            params[1] = 0;
        else
            params[1] = ((BigDecimal) currRow.getAs("avg_cost")).doubleValue();
        params[2] = ((BigDecimal) currRow.getAs("divid_income")).doubleValue();
        params[3] = ((BigDecimal) currRow.getAs("total_income")).doubleValue();
        params[4] = 0;

        Row ret = new GenericRowWithSchema(new Object[]{
                // return RowFactory.create(
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("trd_it_trx_serialno")),//1
                currRow.getAs(st.fieldIndex("dk_tano")),//2
                currRow.getAs(st.fieldIndex("cserialno")), //3
                currRow.getAs(st.fieldIndex("requestno")), //4
                currRow.getAs(st.fieldIndex("sk_invpty_of_cust")), //5
                currRow.getAs(st.fieldIndex("dk_cust_type")), //6
                currRow.getAs(st.fieldIndex("sk_account_of_fd")),// 7
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //8
                currRow.getAs(st.fieldIndex("sk_currency")), //9
                currRow.getAs(st.fieldIndex("agencyno")), //10
                currRow.getAs(st.fieldIndex("netno")), //11
                currRow.getAs(st.fieldIndex("sk_product")),//12
                currRow.getAs(st.fieldIndex("dk_share_type")), //13
                currRow.getAs(st.fieldIndex("sk_agency")), //14
                tranRow.getAs("ta_cfm_date"), //effective_to //15,
                currRow.getAs(st.fieldIndex("net_value")),//16
                currRow.getAs(st.fieldIndex("sk_mkt_trade_type")),//17
                currRow.getAs(st.fieldIndex("dk_bourseflag")),//18
                currRow.getAs(st.fieldIndex("share_change")),//19
                currRow.getAs(st.fieldIndex("shares")),//20
                currRow.getAs(st.fieldIndex("avg_cost")),//21
                currRow.getAs(st.fieldIndex("divid_income")),//22
                currRow.getAs(st.fieldIndex("divid_income_out")),//23
                currRow.getAs(st.fieldIndex("total_income")),//24
                currRow.getAs(st.fieldIndex("total_fee")),//25
                currRow.getAs(st.fieldIndex("dk_is_valid")),//26
                currRow.getAs(st.fieldIndex("sdata_serialno")),//27
                "set effective_to=ta_cfm_date",//28
                currRow.getAs(st.fieldIndex("ddvc")),//29
                currRow.getAs(st.fieldIndex("batchno")),//30
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //31
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //32
                tranRow.getAs("dk_system_of_sdata"),//null,33
                currRow.getAs(st.fieldIndex("effective_from")),//34 effective_from
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//35
        }, st);
        return ret;
    }

    static Row changeEt2(Row tranRow, StructType st, double[] params, List<String> transferTypesList) {
        double shares = params[0];
        double avg_cost = params[1];
        double divid_income = params[2];
        double total_income = params[3];
        double zero_flag = params[4];//是否全部赎回,暂时没有用

        double new_shares = 0;
        double new_avg_cost = 0;
        double new_divid_income = 0;
        double new_total_income = 0;
        double divid_income_out = 0;//托管转出带走的分红

        double netvalue = ((BigDecimal) tranRow.getAs("net_value")).doubleValue();
        double tr_shares = ((BigDecimal) tranRow.getAs("confirm_shares")).doubleValue();
        double tr_amount = ((BigDecimal) tranRow.getAs("confirm_balance")).doubleValue();
        double tr_avg_cost = ((BigDecimal) tranRow.getAs("avg_cost")).doubleValue();
        double tr_divid_income_out = ((BigDecimal) tranRow.getAs("divid_income_out")).doubleValue();
        double tr_total_fee = ((BigDecimal) tranRow.getAs("total_fee")).doubleValue();
        int incomerule = ((BigDecimal) tranRow.getAs("income_rule")).intValue();
        int shr_chg_dir = ((BigDecimal) tranRow.getAs("shr_chg_dir")).intValue();
        int sk_mkt_trade_type = ((BigDecimal) tranRow.getAs("sk_mkt_trade_type")).intValue();
        String dk_bonus_type = (String) tranRow.getAs("dk_bonus_type");
        //System.out.println("sk_mkt_trade_type="+sk_mkt_trade_type+",dk_bonus_type="+dk_bonus_type);
        //'交易增减标识；0-份额不增不减，1-份额增加，-1-份额减少',
        //'盈亏计算规则\;0-不计入盈亏,1-交易确认金额计入收益,2-根据净值计算确认金额计入收益,-1-交易确认金额计入成本,-2-根据净值计算确认金额计入收益',

        double tr_cost = 0;
        if (shr_chg_dir == 1)//增加份额
        {
            if (sk_mkt_trade_type == 143)//分红按增加份额处理，但是收益规则处理写死
            {
                //同一天有分红，也有赎回的可能吗
                if ("1".equals(dk_bonus_type))//现金分红
                {
                    new_shares = shares;
                    new_avg_cost = avg_cost;
                    new_divid_income = divid_income + tr_amount;
                    //System.out.println("new_divid_income="+new_divid_income);
                    new_total_income = total_income;
                } else//红利再投
                {
                    new_shares = shares + tr_shares;
                    tr_cost = tr_shares * netvalue;
                    new_avg_cost = (avg_cost * shares + tr_cost) / new_shares;
                    new_divid_income = divid_income;
                    new_total_income = total_income;
                }
            } else {
                if (transferTypesList.contains(String.valueOf(sk_mkt_trade_type)))//转托管的算法不同
                {
                    tr_cost = tr_shares * tr_avg_cost;
                } else if (incomerule == -1) {
                    tr_cost = tr_amount;
                } else if (incomerule == -2) {
                    tr_cost = tr_shares * netvalue;
                }
                new_shares = shares + tr_shares;
                new_avg_cost = (avg_cost * shares + tr_cost + (sk_mkt_trade_type == 137 ? tr_total_fee : 0)) / new_shares;//基金转换入，算上补差费用
                new_divid_income = divid_income + tr_divid_income_out;
                new_total_income = total_income;
            }
        } else if (shr_chg_dir == -1)//减少份额
        {
            if (transferTypesList.contains(String.valueOf(sk_mkt_trade_type)))//转托管的算法不同
            {
                tr_cost = 0;//tr_shares*avg_cost;卖掉份额产生的收益
                divid_income_out = divid_income * (tr_shares / shares);//带走分红
            } else if (incomerule == 1) {
                tr_cost = tr_amount - tr_shares * avg_cost;
            } else if (incomerule == 2) {
                tr_cost = tr_shares * (netvalue - avg_cost);
            }

            if (shares - tr_shares < err)//份额清零
            {
                zero_flag = 1;
                new_divid_income = 0;
                new_total_income = total_income + (divid_income - divid_income_out) + tr_cost;
            } else {
                new_divid_income = divid_income * (1 - tr_shares / shares);
                new_total_income = total_income + (divid_income * (tr_shares / shares) - divid_income_out) + tr_cost;
            }
            new_shares = shares - tr_shares;
            new_avg_cost = avg_cost;
        }

        if (!Double.isFinite(new_avg_cost)) {
            new_avg_cost = -1;
        }
        if (!Double.isFinite(divid_income_out)) {
            divid_income_out = 0;
            System.out.println("divid_income_out,sk_account_of_fd=" + tranRow.getAs("sk_account_of_fd"));
        }
        if (!Double.isFinite(new_total_income)) {
            new_total_income = 0;
            System.out.println("new_total_income,sk_account_of_fd=" + tranRow.getAs("sk_account_of_fd"));
        }
        if (!Double.isFinite(tr_shares)) {
            tr_shares = 0;
            System.out.println("tr_shares,sk_account_of_fd=" + tranRow.getAs("sk_account_of_fd"));
        }

        params[0] = new_shares;
        params[1] = new_avg_cost;
        params[2] = new_divid_income;
        params[3] = new_total_income;
        params[4] = zero_flag;


        Row ret = new GenericRowWithSchema(new Object[]{
                //return RowFactory.create(
                tranRow.getAs("hashcode"),//0
                null,//1
                tranRow.getAs("dk_tano"),//2
                tranRow.getAs("cserialno"), //3
                tranRow.getAs("requestno"), //4
                tranRow.getAs("sk_invpty_of_cust"), //5
                tranRow.getAs("dk_cust_type"), //6
                tranRow.getAs("sk_account_of_fd"),// 7
                tranRow.getAs("sk_tradeacco_reg"), //8
                tranRow.getAs("sk_currency"), //9
                tranRow.getAs("agencyno"), //10
                tranRow.getAs("netno"), //11
                tranRow.getAs("sk_product"),//12
                tranRow.getAs("dk_share_type"), //13
                tranRow.getAs("sk_agency"), //14
                new BigDecimal("99991231"), //effective_to 15
                tranRow.getAs("net_value"), //16
                tranRow.getAs("sk_mkt_trade_type"), //17
                tranRow.getAs("dk_bourseflag"), //18
                new BigDecimal(tr_shares * shr_chg_dir), //share_change 19
                new BigDecimal(new_shares), //shares 20 剩余份额
                //new BigDecimal(Double.parseDouble(new DecimalFormat("#.00000000").format(new_avg_cost))), // 21
                new BigDecimal(new_avg_cost),
                new BigDecimal(new_divid_income), //income 22
                new BigDecimal(divid_income_out), //income 23
                new BigDecimal(new_total_income), //total_income 24
                tranRow.getAs("total_fee"), //25
                null,//dk_is_valid 26
                null,//sdata_serialno 27
                "new",//memo 28
                null,//ddvc 29
                tranRow.getAs("batchno"), // 30
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //31
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //32
                tranRow.getAs("dk_system_of_sdata"),//33
                tranRow.getAs("ta_cfm_date"), //effective_from 34,
                tranRow.getAs("dk_system_of_sdata")//dk_system_of_sdata 35
        }, st
        );
        return ret;
    }
}
