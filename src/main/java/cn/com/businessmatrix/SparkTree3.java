package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkTree3 {

    public static long val = 0;
    public static List<Row> list = null;
    public static Map<Long, List<Long>> map = null;

    public static void main(String[] args) {

        String srcTableName = args[0];
        String keyColName = args[1];
        String pKeyColName = args[2];
        String tgtTableName = args[3];
        String startNum = args[4];
        String rootNum = args[5];
        String whereClause = args[6];
        val = Long.parseLong(startNum);

        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .appName("SparkTree3")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();

        list = new ArrayList<Row>();
        //rootNum 是第一个id的值，它的上级强制为空
        String sql = "select " + keyColName + " as id,case when " + keyColName + "=" + rootNum + " then null else " + pKeyColName + " end as pid,cast(0 as decimal(12,0)) as lval,cast(0 as decimal(12,0)) as rval from "
                + srcTableName + "  where " + whereClause + " order by id";

        System.out.println(sql);
        Dataset<Row> adf = hc.sql(sql);

        long num = adf.count();
        Long key = null;
        Row[] rs = null;
        if (num > 0) {
            rs = (Row[]) adf.take((int) num);
            map = new HashMap<Long, List<Long>>();
            List<Long> ls = null;
            for (int i = 0; i < num; i++) {
                if (rs[i].isNullAt(1)) {
                    key = null;
                } else {
                    key = rs[i].getDecimal(1).longValue();
                }
                ls = map.get(key);
                if (ls == null)
                    ls = new ArrayList<Long>();
                ls.add(rs[i].getDecimal(0).longValue());
                map.put(key, ls);
            }
            handleNext(null);
        }

        StructType schema = hc.sql("select cast(0 as decimal(12,0)) as " + keyColName
                + ",cast(0 as decimal(12,0)) as lval,cast(0 as decimal(12,0)) as rval")
                .schema();
        hc.createDataFrame(list, schema).write().format("parquet")
                //.option("parquet.compression","SNAPPY")
                .mode(SaveMode.Overwrite).saveAsTable(tgtTableName);
        hc.close();
    }

    static void handleNext(Long pid) {
        long id = 0;
        long lval = 0;
        long rval = 0;
        List<Long> lst = map.get(pid);
        if (lst != null) {
            int num = lst.size();
            for (int i = 0; i < num; i++) {
                id = lst.get(i);
                val = val + 1;
                lval = val;
                handleNext(id);
                val = val + 1;
                rval = val;
                list.add(
                        RowFactory.create(
                                new BigDecimal(id),
                                new BigDecimal(lval),
                                new BigDecimal(rval)
                        ));
            }
        }
    }
}
