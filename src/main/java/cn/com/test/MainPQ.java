package cn.com.test;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/*
 *  盈亏-平均成本法,先处理在startdate,enddate期间有双向交易的账户
 *   
 *     
 *  参数顺序：
 *   1：	srcsys=args[0];//系统
	 2：	sparktable=args[1];//存放spark临时数据的表
     3： startdate=args[2];//开始日期
	 4： enddate=args[3];//结束日期	
	 5： schema=args[4];//schema
	 6： sparkname=args[5];//spark任务名称，方便查找
	 7： step=args[6];//对应的步骤。
	 8:  midtable=args[7];//交易表加工后作为中间表输入。
	 9:  savemode=args[8];//spark将结果写入sparktable的模式,append 和overwrite。
 *  
 *  
 */
public class MainPQ {

    static String rootPath = "F:\\ideaProducts\\JavaSpark\\src\\main\\resources\\data\\";

    public static void main(String[] args) throws Exception {

        //设置Hadoop执行环境
        SetHadoopDev.init();

        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .appName("ddd")
                .master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();


        String file = "trd_ta_saletran";
        Dataset<Row> lookdf = hc.read().parquet(rootPath+ "parquet\\" + file);
        lookdf.printSchema();
        lookdf.show();
        ///System.out.println(lookdf.show());
        lookdf.registerTempTable("aa");
        //System.out.println(hc.sql("select * from aa where partition_sys='HSTA'").count());

        //hc.createDataFrame(lookdf.toJavaRDD(), lookdf.schema()).write()
        //..format("csv")
        //.mode( SaveMode.Append ).save("/Users/Jack/Documents/workspace/spark2/src/org/hesc/example/csv/");


    }
}
