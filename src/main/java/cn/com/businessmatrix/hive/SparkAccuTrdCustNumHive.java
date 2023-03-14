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
public class SparkAccuTrdCustNumHive {


    public static void main(String[] args) throws Exception {

        String sparktable = args[0];//存放spark临时数据的表
        String startdate = args[1];
        String enddate = args[2];
        String schema_pub = args[3];
        String schema_lbl = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String beforedate = args[6];
        String postfix = "";
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

        String sql_lookdf = "select dk_amount_range,idx_val,bus_date, idx_code from " + schema_lbl + "." + sparktable + " where 1=2 ";
        String sql = " select case 		 when p.confirm_balance<=cast(10000   as decimal(22,6))    then 'PCH_AMT01' " +
                " when p.confirm_balance<=cast(50000   as decimal(22,6))    then 'PCH_AMT02' " +
                " when p.confirm_balance<=cast(100000  as decimal(22,6))    then 'PCH_AMT03' " +
                " when p.confirm_balance<=cast(200000  as decimal(22,6))    then 'PCH_AMT04' " +
                " when p.confirm_balance<=cast(500000  as decimal(22,6))    then 'PCH_AMT05' " +
                " when p.confirm_balance<=cast(1000000 as decimal(22,6))    then 'PCH_AMT06' " +
                " when p.confirm_balance<=cast(5000000 as decimal(22,6))    then 'PCH_AMT07' " +
                " else 'PCH_AMT08'  end as dk_amount_range," +
                " cast(count(distinct p.bk_invpty_of_cust) as decimal(8,0)) as idx_val," +
                " p.sk_date as bus_date,'57' as idx_code " +
                " from (" +
                " (  select t.bk_invpty_of_cust,c.sk_date,cast(sum(t.confirm_balance) as decimal(22,6)) as confirm_balance from " +
                " ( select t1.cfm_date,t1.bk_invpty_of_cust,cast(sum(confirm_balance) as decimal(22,6)) as confirm_balance " +
                "  from " + schema_pub + ".flt_trd_cust_saletran" + postfix + "  t1 left join " +
                schema_pub + ".flt_prod_product" + postfix + "  	 t2 on t1.sk_product=t2.sk_product " +
                " and t2.bus_date =  " + beforedate +
                " where t1.sk_trade_type_of_mkt in (120,122,137,139) " +
                " and t1.dk_saletran_status ='1' " +
                " and t2.dk_product_type  in('01','02') " +
                " and t1.cfm_date_m <= floor(" + enddate + "/100) " +
                " group by t1.cfm_date,t1.bk_invpty_of_cust " +
                " ) t " +
                " left join " +
                "( select cfm_date as sk_date from  " +
                schema_lbl + ".tmp_idx_endtm_rundate " +
                " ) c on 1=1 " +
                " where t.cfm_date<=c.sk_date " +
                " group by c.sk_date,t.bk_invpty_of_cust ) p " +
                ") group by p.sk_date," +
                "case   when p.confirm_balance<=cast(10000   as decimal(22,6))    then 'PCH_AMT01' " +
                " when p.confirm_balance<=cast(50000   as decimal(22,6))    then 'PCH_AMT02' " +
                " when p.confirm_balance<=cast(100000  as decimal(22,6))    then 'PCH_AMT03' " +
                " when p.confirm_balance<=cast(200000  as decimal(22,6))    then 'PCH_AMT04' " +
                " when p.confirm_balance<=cast(500000  as decimal(22,6))    then 'PCH_AMT05' " +
                " when p.confirm_balance<=cast(1000000 as decimal(22,6))    then 'PCH_AMT06' " +
                " when p.confirm_balance<=cast(5000000 as decimal(22,6))    then 'PCH_AMT07' " +
                " else 'PCH_AMT08'  end ";


        System.out.println(sql_lookdf);
        System.out.println();
        System.out.println(sql);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        Dataset<Row> adf = hc.sql(sql);

        hc.createDataFrame(adf.javaRDD().coalesce(1), lookdf.schema()).write()
                .format("hive")
                .partitionBy("idx_code")
                .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                .saveAsTable(schema_lbl + "." + sparktable);

        String sql2 = "alter table " + schema_lbl + ".agg_idx_val_d drop if exists partition(idx_code='57',bus_date>=" + startdate + ",bus_date<=" + enddate + ")";
        System.out.println();
        System.out.println(sql2);
        //hc.sql(sql2);


        sql2 = "insert overwrite table " + schema_lbl + ".agg_idx_val_d partition(idx_code,bus_date) "
                + " select "
                + "null as dk_cust_type,"
                + "null as dk_age_range,"
                + "null as dk_open_age_range,"
                + "null as dk_sex,"
                + "null as sk_product,"
                + "null as bk_product,"
                + "null as product_short_name,"
                + "null as sk_agency,"
                + "null as agencyno,"
                + "null as agencyno_name,"
                + "null as sk_region,"
                + "dk_amount_range,"
                + "'累计交易客户数-金额段' as idx_name,"
                + "idx_val,"
                + "cast(current_timestamp() as string) as inserttime,"
                + "cast(current_timestamp() as string) as updatetime,"
                + "idx_code,bus_date "
                + " from " + schema_lbl + "." + sparktable
        ;
        System.out.println();
        System.out.println(sql2);
        //hc.sql(sql2);

        hc.close();

    }

}
