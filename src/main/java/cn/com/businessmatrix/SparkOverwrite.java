package cn.com.businessmatrix;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkOverwrite {

    public static void main(String[] args) throws Exception {

        String sparktable1 = args[0];
        String sparktable2 = args[1];
        String sparkname = args[2];

        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder().config(conf)
                .enableHiveSupport().appName(sparkname).getOrCreate();

        hc.table(sparktable1).write().format("parquet").partitionBy("dk_system_of_sdata")
                .mode(SaveMode.Overwrite).saveAsTable(sparktable2);

        hc.close();
    }
}
