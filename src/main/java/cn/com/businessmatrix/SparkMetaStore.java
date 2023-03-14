package cn.com.businessmatrix;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkMetaStore {

    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .appName("MyApacheSparkTest")
                .master("local[*]")
                .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:/Users/Jack/Documents/workspace/spark2/metastore_db;create=false")
                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
                //.config("spark.hadoop.javax.jdo.option.ConnectionUserName","APP")
                //.config("spark.hadoop.javax.jdo.option.ConnectionPassword","mine")
                .enableHiveSupport()
                .getOrCreate();

        //hc.sql("create database mydb");

        String ds = "/Users/Jack/Documents/workspace/spark2/resources/test.js";

        Dataset<Row> df = hc.read().json(ds);
        df.write().saveAsTable("mydb.airline_to_country2");

        hc.close();
    }
}
