package cn.com.businessmatrix;

import java.math.BigDecimal;
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
 *  第三步骤：对份额减少情况进行处理
 *  参数顺序：
 *  	第一个参数：tano
 *  	第二个参数：batchno
 *  	第三个参数：startdate
 *      第四个参数：enddate
 *      第五个参数：schema
 *      第六个参数：step
 *  要求：
 *   1.表。
 *
 */
public class SparkPL4 {

    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {

        String srcsys = args[0];
        String batchno = args[1];
        String startdate = args[2];
        String enddate = args[3];
        String schema = args[4];
        int step = Integer.parseInt(args[5]);

        Date d_startdate = sdf.parse(startdate);
        Date d_enddate = sdf.parse(enddate);
        Date d_sp_startdate = d_startdate;
        Date d_sp_enddate = null;

        Map<String, String> dBConOption = new HashMap<String, String>();
        dBConOption.put("url", "jdbc:impala:@10.64.35.70:25003/ods;AuthMech=3");
        dBConOption.put("user", "etl_az");
        dBConOption.put("password", "123456");
        dBConOption.put("driver", "com.cloudera.impala.jdbc41.Driver");


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
                .appName("SparkPL2")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();
        ;
        DataFrameReader dfRead = hc.read().format("jdbc").options(dBConOption);


        while (true) {


            //" distribute by hashcode "+
            //" sort by reg_date asc, ori_cserialno asc"
            ;

            Dataset<Row> lookdf = dfRead.option("dbtable", "vw_ass_ta_cust_income_detail").load();
            lookdf = lookdf.filter(lookdf.col("dk_system_of_upd").equalTo(srcsys))
                    .repartition(lookdf.col("hashcode"))
                    .orderBy("reg_date", "ori_cserialno");

            StructType st = lookdf.schema();

            if (d_sp_startdate.after(d_enddate))
                break;
            d_sp_enddate = add(d_sp_startdate, step);
            if (d_sp_enddate.after(d_enddate))
                d_sp_enddate = d_enddate;

            Dataset<Row> adf = dfRead.option("dbtable", "vw_trd_ta_saletran").load();
            adf = adf.filter(adf.col("ta_cfm_date").between(sdf.format(d_sp_startdate), sdf.format(d_sp_enddate)))
                    .repartition(adf.col("hashcode"))
                    .orderBy("ta_cfm_date", "cserialno");
            ;

            JavaRDD<Iterable<Row>> w = handleNext2(adf, lookdf, st);
            JavaRDD<Row> x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
                private static final long serialVersionUID = -6111647437563220114L;

                @Override
                public java.util.Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                    return arg0.iterator();
                }

            });

            //System.out.println(x.count());
            //hc.sql("truncate table "+schema+".mid_fact_custincomechg_detail");
            hc.createDataFrame(x, lookdf.schema()).registerTempTable("c");
            hc.sql("insert into table  " + schema + ".mid_ass_ta_cust_income_detail  select * from c");
            //hc.createDataFrame(x, lookdf.schema()).write().format("parquet").mode(SaveMode.Overwrite)
            //.saveAsTable(schema+".mid_ass_ta_cust_income_detail");
            //hc.refreshTable(schema+".mid_fact_custincomechg_detail");
            //System.out.println("count="+hc.sql("select * from test.mid_fact_custincomechg_detail").count());

            d_sp_startdate = add(d_sp_enddate, step);
        } //end while

        //Thread.currentThread().sleep(1000*1000L);
        hc.close();

    }


    static JavaRDD<Iterable<Row>> handleNext2(Dataset<Row> adf, final Dataset<Row> lookdf, StructType st) {

        JavaPairRDD<Object, Row> ardd = adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();
        JavaPairRDD<Object, Row> lkprdd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> result = ardd.cogroup(lkprdd);


        JavaRDD<Iterable<Row>> w = result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {

            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                //System.out.println("===hashcode:"+tuple._1);
                List<Row> ls = new ArrayList<Row>();
                Iterable<Row> tr = tuple._2._1;
                Iterable<Row> vr = tuple._2._2;

                java.util.Iterator<Row> itr = tr.iterator();
                java.util.Iterator<Row> ivr = vr.iterator();

                Row t = null;
                Row f = null;
                double rmshares = 0;
                boolean findNext = true;
                // StructType st=null;
                while (itr.hasNext()) {
                    t = itr.next();
                    rmshares = ((BigDecimal) t.getAs("confirm_shares")).doubleValue();
                    double[] d = new double[]{rmshares, 0};
                    while (rmshares > 0) {
                        //System.out.println("rmshares="+rmshares);
                        if (findNext) {
                            if (ivr.hasNext()) {
                                f = ivr.next();
                                //st=f.schema();
                            } else {
                                break;
                            }
                        }
                        ls.add(changeEt(f, t, st));
                        Row ret = changeEt2(f, t, d, st);


                        if (((BigDecimal) ret.getAs(st.fieldIndex("shares"))).doubleValue() < 0.000001) {
                            findNext = true;
                            //ret.
                            ls.add(ret);
                        } else {
                            f = ret;
                            findNext = false;
                        }

                        rmshares = d[0];
                    }
                }
                return ls;
            }
        });

        return w.filter(new Function<Iterable<Row>, Boolean>() {
            private static final long serialVersionUID = -430944040308979866L;

            @Override
            public Boolean call(Iterable<Row> arg0) throws Exception {
                if (arg0.iterator().hasNext())
                    return true;
                else
                    return false;
            }

        });
    }


    static Row changeEt2(Row r, Row tr, double[] d, StructType st) {
        double rmshares = d[0];
        double tmptotalincome = d[1];
        double orinetvalue = ((BigDecimal) r.getAs(st.fieldIndex("ori_net_value"))).doubleValue();
        double netvalue = ((BigDecimal) tr.getAs("net_value")).doubleValue();
        double tr_shares = ((BigDecimal) tr.getAs("confirm_shares")).doubleValue();
        double tr_amount = ((BigDecimal) tr.getAs("confirm_balance")).doubleValue();
        double old_shares = ((BigDecimal) r.getAs(st.fieldIndex("shares"))).doubleValue();
        double new_shares = 0;
        if (old_shares - rmshares >= 0)
            new_shares = old_shares - rmshares;
        else
            new_shares = 0;
        double shrchg = 0;
        if (old_shares <= rmshares)
            shrchg = old_shares;
        else
            shrchg = rmshares;
        rmshares += (-1.0 * shrchg);


        double incomerule = ((BigDecimal) tr.getAs("income_rule")).doubleValue();

        double costofincome = 0;
        double oricost = ((BigDecimal) r.getAs(st.fieldIndex("ori_cost"))).doubleValue();
        double orishares = ((BigDecimal) r.getAs(st.fieldIndex("ori_shares"))).doubleValue();
        double totalcostofincome = ((BigDecimal) r.getAs(st.fieldIndex("total_income_incld_cost"))).doubleValue();
        double income = 0;

        if (incomerule == 1) {
            if (oricost - totalcostofincome > 0) {
                costofincome = Double.parseDouble(new DecimalFormat("#.00").format(shrchg / orishares * oricost));
            }

            if (rmshares > 0) {
                income = shrchg / tr_shares * tr_amount;
                tmptotalincome += income;
            } else {
                income = tr_amount - tmptotalincome;
            }

        } else if (incomerule == 2) {

            costofincome = shrchg * orinetvalue;
            income = shrchg * netvalue;
        } else if (incomerule == 0) {
            costofincome = 0;
            income = 0;
        }
        double totalincome = ((BigDecimal) r.getAs(st.fieldIndex("total_income"))).doubleValue() + income;
        totalcostofincome += costofincome;
        d[0] = rmshares;
        d[1] = tmptotalincome;

        return RowFactory.create(
                r.getAs(st.fieldIndex("hashcode")),//
                null,//
                r.getAs(st.fieldIndex("dk_tano")),//
                tr.getAs("cserialno"), //
                r.getAs(st.fieldIndex("ori_cserialno")), //
                r.getAs(st.fieldIndex("sk_invpty_of_cust")), //
                r.getAs(st.fieldIndex("dk_cust_type")), //
                r.getAs(st.fieldIndex("sk_account_of_fd")),//
                r.getAs(st.fieldIndex("sk_tradeacco_reg")), //
                r.getAs(st.fieldIndex("sk_currency")), //
                r.getAs(st.fieldIndex("agencyno")), //
                r.getAs(st.fieldIndex("netno")), //
                r.getAs(st.fieldIndex("sk_product")),//
                r.getAs(st.fieldIndex("dk_share_type")), //
                r.getAs(st.fieldIndex("sk_agency")), //
                r.getAs(st.fieldIndex("reg_date")), //
                new BigDecimal("99991231"), //effective_to
                r.getAs(st.fieldIndex("ori_net_value")), //
                r.getAs(st.fieldIndex("ori_sk_mkt_trade_type")), //
                tr.getAs("sk_mkt_trade_type"), //
                tr.getAs("dk_bourseflag"), //
                r.getAs(st.fieldIndex("ori_shares")), //
                r.getAs(st.fieldIndex("ori_cost")), //
                r.getAs(st.fieldIndex("shares")), //last_shares
                new BigDecimal(-1.0 * shrchg), //share_change
                new BigDecimal(new_shares), //shares
                new BigDecimal(0), //cost
                new BigDecimal(income), //income
                new BigDecimal(costofincome), //income_incld_cost
                r.getAs(st.fieldIndex("total_cost")), //total_cost
                new BigDecimal(totalincome), //total_income
                new BigDecimal(totalcostofincome), //total_income_incld_cost
                tr.getAs("back_fee"),
                null,//dk_is_valid
                null,//dk_system_of_sdata
                null,//sdata_serialno
                "new",//memo
                null,//ddvc
                tr.getAs("batchno"), //
                new java.sql.Timestamp(System.currentTimeMillis()),
                new java.sql.Timestamp(System.currentTimeMillis()),
                tr.getAs("dk_system_of_upd"),
                tr.getAs("ta_cfm_date") //effective_from
        );
    }


    static Row changeEt(Row r, Row tr, StructType st) {
        return RowFactory.create(
                r.getAs(st.fieldIndex("hashcode")),//
                r.getAs(st.fieldIndex("trd_it_trx_serialno")),//
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                tr.getAs("ta_cfm_date"), //effective_to
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new java.sql.Timestamp(System.currentTimeMillis()),
                null,
                null
        );
    }

    static Date add(Date d, int days) {
        return new Date(d.getTime() + days * 24 * 60 * 60 * 1000L);
    }
}
