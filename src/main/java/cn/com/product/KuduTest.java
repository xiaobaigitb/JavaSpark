package cn.com.product;


import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class KuduTest {

    /*
     * 管理区划拆分
     */

    public static void main(String[] args) {

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
                .appName("tttttt")
                //.master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();


        //	配置kudu
        String kudu_master = "cdh03.businessmatrix.com.cn:7051";
//		String table_name = "impala::aml_dm_kudu.trd_fundbal_detail";
//		SparkConf conf = new SparkConf();
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		HiveContext hc = new HiveContext(sc);
//		
//		StructType kudust = StructTypeUtils.getStructType("tmp_trd_fundbal_detail");
//		hc.read().format("org.apache.kudu.spark.kudu").schema(kudust)
//		.option("kudu.master","dp-dsj-n21:7051,dp-dsj-n22:7051,dp-dsj-n23:7051")
//		.option("kudu.table","impala::aml_dm_kudu.trd_fundbal_detail")
//		.load();

        Map<String, String> map = new HashMap<String, String>();
        map.put("kudu.master", "cdh03.businessmatrix.com.cn:7051");
        map.put("kudu.table", "rls_ods.MDM_IP_INVPTY_MAPP");
        Dataset df = hc.read().options(map).format("kudu").load();
        df.registerTempTable("my_table");

        hc.sql("select * from my_table").show();

        //转为DataFrame
        df.printSchema();


        hc.close();
    }
}
