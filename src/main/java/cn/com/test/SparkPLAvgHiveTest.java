package cn.com.test;

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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/*
 * 测试精度超长不能写入的问题
 *
 *
 */
public class SparkPLAvgHiveTest {
    public static final double err = 0.000001d;
    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        String schema = args[0];
        String sparktable = args[1];
        String sparkname = args[2];

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
        StructType st = null;
        List<StructField> schemaFields = null;
        {
            schemaFields = new ArrayList<StructField>();
            schemaFields.add(DataTypes.createStructField("d1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("d2", DataTypes.createDecimalType(10, 5), true));

            st = DataTypes.createStructType(schemaFields);
        }

        Row ret = new GenericRowWithSchema(new Object[]{
                // return RowFactory.create(
                "629843534534535.53333",
                new BigDecimal(629843534534535.53333d)},
                st
        );
        List<Row> list = new ArrayList<Row>();
        list.add(ret);
        hc.createDataFrame(list, st).write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable(schema + "." + sparktable);
        hc.close();

    }
}
