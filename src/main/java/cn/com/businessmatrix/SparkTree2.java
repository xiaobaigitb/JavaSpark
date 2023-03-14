package cn.com.businessmatrix;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.api.java.function.Function;

public class SparkTree2 {


    public static long val = 0;
    public static Row[] rs = null;
    public static List<Long[]> list = null;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        // .setAppName(appName).setMaster(master)
        // .set("spark.hadoop.yarn.resourcemanager.hostname",
        // "resourcemanager.fqdn")
        // .set("spark.hadoop.yarn.resourcemanager.address",
        // "resourcemanager.fqdn:8032")
        // .set("spark.hadoop.fs.defaultFS","hdfs://172.39.32.81:8020")
        // .set("spark.driver.host", "172.39.33.92")
        // .set("spark.yarn.jar",
        // "hdfs://cdh03.businessmatrix.com.cn:8020/user/oozie/share/lib/lib_20200213101250/spark")
        // .set("spark.hadoop.yarn.resourcemanager.hostname",
        // "cdh03.businessmatrix.com.cn")
        // .set("spark.hadoop.yarn.resourcemanager.address","cdh03.businessmatrix.com.cn:8032");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(sc);
        list = new ArrayList<Long[]>();
        Dataset<Row> adf = hc
                .sql("select sk_agency as id,sk_agency_parent as pid,0 as lval,0 as rval from test.ip_agency_hierarchy t");

        long num = adf.count();
        if (num > 0) {
            rs = (Row[]) adf.take((int) num);
            handleNext(null);
        }

        sc.parallelize(list).map(new Function<Long[], String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Long[] v) throws Exception {
                return v[0] + "," + v[1] + "," + v[2];
            }
        }).saveAsTextFile("/tmp/sparktxt" + System.currentTimeMillis());
        sc.close();
    }

    static void handleNext(Long pid) {
        int num = rs.length;
        long id = 0;
        long lval = 0;
        long rval = 0;
        for (int i = 0; i < num; i++) {
            if (pid == null) {
                if (rs[i].isNullAt(1))
                    id = rs[i].getDecimal(0).longValue();
                else
                    continue;
            } else {
                if (rs[i].isNullAt(1))
                    continue;
                else if (rs[i].getDecimal(1).longValue() == pid)
                    id = rs[i].getDecimal(0).longValue();
                else
                    continue;
            }
            val = val + 1;
            lval = val;
            handleNext(id);
            val = val + 1;
            rval = val;
            list.add(new Long[]{id, lval, rval});
        }
    }
}
