package cn.com.businessmatrix;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class SparkTree {

    public static long val = 0l;
    public static Dataset<Row> udf = null;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(sc);

        Dataset<Row> adf = hc
                .sql("select sk_region as id,sk_region_parent as pid,0 as lval,0 as rval,0 as flag  from test.comm_region_hierarchy t");
        udf = adf.filter("id=-100");
        Dataset<Row> sdf = adf.filter("pid is null");
        handleNext(adf, sdf);
        udf.write().mode(SaveMode.Overwrite).saveAsTable("test.sparktest");
        sc.close();
    }

    static void handleNext(Dataset<Row> adf, Dataset<Row> df) {
        long num = df.count();
        long lval = 0;
        long rval = 0;
        if (num > 0) {
            Row[] r = (Row[]) df.take((int) num);
            for (int i = 0; i < num; i++) {
                long id = r[i].getDecimal(0).longValue();
                Dataset<Row> rdf = adf.filter("pid = " + id);
                val = val + 1;
                lval = val;
                handleNext(adf, rdf);
                val = val + 1;
                rval = val;
                Dataset<Row> x = adf.filter("id=" + id);
                Column cl = x.col("lval").plus(lval);
                Column cr = x.col("rval").plus(rval);
                udf = udf.unionAll(x.withColumn("lval", cl).withColumn("rval",
                        cr));
            }
        }
    }

}
