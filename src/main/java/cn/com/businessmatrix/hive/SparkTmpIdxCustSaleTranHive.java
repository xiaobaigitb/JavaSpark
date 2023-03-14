package cn.com.businessmatrix.hive;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*
 *
 *
 *
 */
public class SparkTmpIdxCustSaleTranHive {


    public static void main(String[] args) throws Exception {

        String sparktable = args[0];//存放spark临时数据的表
        String startdate = args[1];
        String enddate = args[2];
        String schema_pub = args[3];
        String schema_lbl = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String beforedate = args[6];

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

        String sql_lookdf = "select * from " + schema_lbl + "." + sparktable + " where 1=2 ";
        hc.sql("truncate table  " + schema_lbl + "." + sparktable);
        System.out.println("truncate table  " + schema_lbl + "." + sparktable);

        String sql = "select " +
                " h.sk_invpty_of_cust " +
                " ,h.dk_cust_type " +
                " ,h.birthday " +
                " ,h.dk_sex " +
                " ,h.dk_id_type " +
                " ,h.std_idno " +
                " ,h.cfm_date " +
                " ,h.bk_invpty_of_cust " +
                " from( " +
                " select w.sk_invpty_of_cust " +
                " ,w.dk_cust_type " +
                " ,w.birthday " +
                " ,w.dk_sex " +
                " ,w.dk_id_type " +
                " ,w.std_idno " +
                " ,w.cfm_date " +
                " ,w.bk_invpty_of_cust " +
                " ,ROW_NUMBER() over(partition by w.bk_invpty_of_cust order by sk_invpty_of_cust desc) as rn " +
                "  from " +
                "  ( " +
                "  select /*+ BROADCAST(t2) */ " +
                " 	t.sk_invpty_of_cust , " +
                " 	t1.dk_cust_type , " +
                "   t1.birthday, " +
                "   t1.dk_sex, " +
                "   t1.dk_id_type, " +
                "   t1.std_idno, " +
                " min(t.cfm_date) as cfm_date , " +
                " t.bk_invpty_of_cust  " +
                " from " +
                " 	" + schema_pub + ".flt_trd_cust_saletran t " +
                " left join " + schema_pub + ".flt_ip_custinfo t1 " +
                "   on t.sk_invpty_of_cust=t1.sk_invpty_of_cust " +
                "  and t1.bus_date=" + beforedate +
                "  left join " + schema_pub + ".flt_prod_product t2 " +
                "   on t.sk_product=t2.sk_product " +
                "  and t2.bus_date=" + beforedate +
                "  where " +
                " 	t.sk_trade_type_of_mkt in( 120, 122, 137, 139) " +
                "  and t.cfm_date_m between floor(" + startdate + "/100) and floor(" + enddate + "/100) " +
                "  and t.dk_saletran_status='1' and t.bk_invpty_of_cust is not null " +
                "  and t2.dk_product_type in('01','02')  " +
                "  group by " +
                " 	t.sk_invpty_of_cust, " +
                " 	t1.dk_cust_type , " +
                "    t1.birthday, " +
                "    t1.dk_sex, " +
                "   t1.dk_id_type, " +
                "   t1.std_idno, " +
                " 	t.bk_invpty_of_cust " +
                "  ) w " +
                "  ) h where h.rn=1 ";
        ;

        System.out.println(sql_lookdf);
        System.out.println();
        System.out.println(sql);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        Dataset<Row> adf = hc.sql(sql);
        adf.repartition(1000, adf.col("bk_invpty_of_cust")).registerTempTable("temp_register_spark");

        String sql2 = " select h.sk_invpty_of_cust " +
                " ,h.dk_cust_type " +
                " ,h.birthday " +
                " ,h.dk_sex " +
                " ,h.dk_id_type " +
                " ,h.std_idno " +
                " ,case when h.cfm_date<nvl(h1.cfm_date,99991231) then h.cfm_date else h1.cfm_date end as cfm_date " +
                " ,null as batchno " +
                " ,null as inserttime " +
                " ,null as updatetime " +
                " ,h.bk_invpty_of_cust " +
                " from " + schema_lbl + ".tmp_idx_cust_saletran_01 h " +
                " left join temp_register_spark h1  " +
                " on h.bk_invpty_of_cust=h1.bk_invpty_of_cust " +
                " union all  " +
                " select  " +
                "  t.sk_invpty_of_cust " +
                " ,t.dk_cust_type " +
                " ,t.birthday " +
                " ,t.dk_sex " +
                " ,t.dk_id_type " +
                " ,t.std_idno " +
                " ,t.cfm_date " +
                " ,null as batchno " +
                " ,null as inserttime " +
                " ,null as updatetime " +
                " ,t.bk_invpty_of_cust " +
                " from temp_register_spark t " +
                " left join " + schema_lbl + ".tmp_idx_cust_saletran_01 t2 " +
                "  on t.bk_invpty_of_cust=t2.bk_invpty_of_cust " +
                " where t2.bk_invpty_of_cust is null";

        Dataset<Row> adf2 = hc.sql(sql2);
        hc.createDataFrame(adf2.javaRDD(), lookdf.schema()).write()
                .format("hive")
                //.partitionBy("bus_date")
                .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                .saveAsTable(schema_lbl + "." + sparktable);


        hc.close();

    }

}
