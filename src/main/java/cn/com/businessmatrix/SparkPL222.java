package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.BoxedUnit;

/*
 *
 *  创建视图
 *  kerberos
 *
 */
public class SparkPL222 {

    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {


        Map<String, String> dBConOption = new HashMap<String, String>();
        //dBConOption.put("url", "jdbc:impala://cdh02:21050/ods;AuthMech=1;");
        dBConOption.put("url", "jdbc:impala://cdh02:21050/ods;AuthMech=1;KrbRealm=businessmatrix.com.cn;KrbHostFQDN=cdh02.businessmatrix.com.cn;KrbServiceName=impala");

        //dBConOption.put("user", "hive");
        //dBConOption.put("password", "123456");
        dBConOption.put("driver", "com.cloudera.impala.jdbc41.Driver");

        createview();
        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .appName("SparkPL2")
                .master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();
        ;
        DataFrameReader dfRead = hc.read().format("jdbc").options(dBConOption);

        String sql_lookdf =
                "(select * from test.vw_x t limit 10) as tt";
        System.out.println(sql_lookdf);
        Dataset<Row> lookdf = dfRead.option("dbtable", "test.vw_x").load();
        lookdf = lookdf//.filter(lookdf.col("dk_system_of_upd").equalTo(srcsys))
                .repartition(lookdf.col("sk_product"))
                .sort(lookdf.col("request_date").asc(), lookdf.col("agencyno").asc());
        JavaPairRDD<Object, Row> ardd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();
        System.out.println(lookdf.collectAsList().size());


    }

    static void createview() {
        Connection connection = null;
        try {
            Class.forName("com.cloudera.impala.jdbc41.Driver");
            connection = DriverManager.getConnection("jdbc:impala://cdh02:21050/ods;AuthMech=1;KrbRealm=businessmatrix.com.cn;KrbHostFQDN=cdh02.businessmatrix.com.cn;KrbServiceName=impala");
            Statement stmt = connection.createStatement();
            stmt.execute("drop view if exists test.vw_x");
            stmt.execute("create view test.vw_x as select * from test.trd_ta_saletran where sk_product=2");
            stmt.close();
            connection.close();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
