package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class Saration {
    public static SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");


    public static void main(String[] args) throws Exception {
    	/*
    	spark2-submit --name spark_saration_test \
		--class cn.com.businessmatrix.Saration \
		--jars /etl/udf/ImpalaJDBC41.jar --master yarn \
		--deploy-mode cluster /etl/udf/spark2-0.0.1-SNAPSHOT_bak1224.jar \
		HSTA4 20201123 20201224 34 1 3 ods spark_saration_procedure_6666
    	*/

        String pi_srcsys = args[0];
        String pi_startdate = args[1];
        String pi_enddate = args[2];
        String pi_tano = args[3];
        String pi_batchno = args[4];
        String pi_closemths = args[5];
        String schema = args[6];
        String sparkname = args[7];
        String sparktable = args[8];
		 
		/*
    	String pi_srcsys = "HSTA4";
    	String pi_startdate = "20201123";
    	String pi_enddate = "20201223";
    	String pi_tano = "34";
    	String pi_batchno = "1";
    	String pi_closemths = "3";
    	String schema = "ods";
    	String sparkname = "spark_saration_procedure_6666";
    	*/

        String v_has_159_agency = "002";
        Integer v_closemths = null;
        if (Integer.parseInt(pi_closemths) <= 0) {
            v_closemths = new Integer(3);
        } else {
            v_closemths = new Integer(Integer.parseInt(pi_closemths));
        }
        final Integer final_v_closemths = v_closemths;


        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .config("hive.execution.engine", "spark")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.dynamic.partition", "true")
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .enableHiveSupport()
                .appName(sparkname)
                //.master("local[*]")
                //.master("yarn-cluster")
                .getOrCreate();


        // load saration
        Dataset<Row> tmp_agrm_saration = hc.sql(
                "SELECT concat_ws('-'," +
                        " cast(t.sk_account_of_fd as string)," +
                        " cast(t.sk_tradeacco_reg as string)," +
                        " cast(t.sk_product as string)," +
                        " cast(t.sk_agency_of_open as string)," +
                        " t.agencyno," +
                        " t.protocolno," +
                        " t.dk_share_type) as hashcode, " +
                        " t.* FROM " + schema + ".agrm_saration t\n" +
                        "  WHERE t.dk_tano = '" + pi_tano + "'\n" +
                        " AND t.dk_system_of_sdata = '" + pi_srcsys + "'\n" +
                        " AND t.open_date < " + pi_startdate);
        StructType agrm_saration_schema = tmp_agrm_saration.schema();

        tmp_agrm_saration = hc.createDataFrame(tmp_agrm_saration.toJavaRDD().map(row -> {
            if (((BigDecimal) row.getAs("close_date")).compareTo(new BigDecimal(pi_startdate)) >= 0) {
                return new GenericRowWithSchema(new Object[]{
                        row.getAs("hashcode"),
                        row.getAs("sk_agreement_of_ration"),
                        row.getAs("sk_product"),
                        row.getAs("dk_share_type"),
                        row.getAs("sk_tradeacco_reg"),
                        row.getAs("dk_tano"),
                        row.getAs("sk_account_of_fd"),
                        row.getAs("bk_fundaccount"),
                        row.getAs("bk_tradeaccount"),
                        row.getAs("agencyno"),
                        row.getAs("netno"),
                        row.getAs("sk_agency_of_open"),
                        row.getAs("balance"),
                        row.getAs("last_balance"),
                        row.getAs("dk_ration_term"),
                        row.getAs("delay"),
                        row.getAs("allow_fail_times"),
                        row.getAs("ration_date"),
                        row.getAs("agio"),
                        "1", //dk_ration_status
                        row.getAs("open_date"),
                        row.getAs("first_date"),
                        row.getAs("last_date"),
                        row.getAs("protocol_end_date"),
                        new BigDecimal("99991231"), //close_date
                        row.getAs("protocol_issue"),
                        row.getAs("protocolno"),
                        row.getAs("dk_is_valid"),
                        row.getAs("sdata_serialno"),
                        row.getAs("memo"),
                        row.getAs("ddvc"),
                        row.getAs("dk_system_of_upd"),
                        row.getAs("batchno"),
                        row.getAs("inserttime"),
                        row.getAs("updatetime"),
                        row.getAs("dk_system_of_sdata")
                }, agrm_saration_schema);
            }
            return row;
        }), agrm_saration_schema);

        //p_agrm_saration_pre
        Dataset<Row> tmp_ration_othagencytran = hc.sql(
                "SELECT concat_ws('-', " +
                        "	cast(t.sk_account_of_fd as string), " +
                        "	cast(t.sk_tradeacco_reg as string), " +
                        "	cast(t.sk_product as string), " +
                        "	cast(t.sk_agency as string), " +
                        "	t.agencyno, " +
                        "	t.protocolno, " +
                        "	t.dk_share_type) as hashcode,\n" +
                        "	t.sk_invpty_of_cust as customerid,\n" +
                        "	t.sk_account_of_fd as sk_fundaccount,\n" +
                        "	t.sk_tradeacco_reg as sk_tradeaccount,\n" +
                        "	t.sk_agency,\n" +
                        "	t.sk_product,\n" +
                        "	t.dk_share_type as sharetype,\n" +
                        "	t.bk_fundaccount,\n" +
                        "	t.bk_tradeaccount,\n" +
                        "	t.cserialno,\n" +
                        "	t.dk_tano as tano,\n" +
                        "	t.agencyno,\n" +
                        "	t.netno,\n" +
                        "	t.ta_cfm_date as sk_confirmdate,\n" +
                        "	t.confirm_balance as amount,\n" +
                        "	t.confirm_shares as shares,\n" +
                        "	nvl(t.protocolno, '-1') as protocolno,\n" +
                        "	t.dk_system_of_upd as srcsys\n" +
                        " FROM " + schema + ".trd_ta_saletran t\n" +
                        "  WHERE t.dk_system_of_sdata = '" + pi_srcsys + "'\n" +
                        " AND t.dk_tano = '" + pi_tano + "'\n" +
                        " AND t.dk_saletran_status = '1'\n" +
                        " AND instr('" + v_has_159_agency + "', t.agencyno, 1) = 0\n" +
                        " AND t.sk_mkt_trade_type = 139 \n" +
                        " AND t.ta_cfm_date BETWEEN " + pi_startdate + " AND " + pi_enddate);

        tmp_ration_othagencytran = tmp_ration_othagencytran.repartition(tmp_ration_othagencytran.col("hashcode"));
        JavaPairRDD<Object, Row> JavaPairRDD_othagencytran = tmp_ration_othagencytran.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());

        tmp_agrm_saration = tmp_agrm_saration.repartition(tmp_agrm_saration.col("hashcode"));
        JavaPairRDD<Object, Row> JavaPairRDD_saration = tmp_agrm_saration.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> othagencytran_cogroup_saration = JavaPairRDD_othagencytran.cogroup(JavaPairRDD_saration);

        JavaRDD<Iterable<Row>> result = othagencytran_cogroup_saration.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {
            private static final long serialVersionUID = -430944040308979866L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                return map_f(tuple, agrm_saration_schema, final_v_closemths, pi_batchno, pi_enddate);
            }//end of call
        });//end of map

        JavaRDD<Row> gen_saration_rdd = result.filter(new Function<Iterable<Row>, Boolean>() {
            private static final long serialVersionUID = -430944040308979866L;

            @Override
            public Boolean call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator().hasNext();
            }
        }).flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator();
            }
        });

        Dataset<Row> gen_saration_table = hc.createDataFrame(gen_saration_rdd, agrm_saration_schema);
        //gen_saration_table.show(100);

        gen_saration_table.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .partitionBy("dk_system_of_sdata")
                .saveAsTable(schema + "." + sparktable);

        hc.close();
    }

    public static Iterable<Row> map_f(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple,
                                      StructType agrm_saration_schema,
                                      Integer final_v_closemths,
                                      String pi_batchno,
                                      String pi_enddate) throws Exception {
        Iterator<Row> itr_othagencytran = tuple._2._1.iterator();
        Iterator<Row> itr_row_saration = tuple._2._2.iterator();
		/* p_agrm_saration(pi_startdate IN INTEGER,
							pi_enddate   IN INTEGER,
							pi_closemths IN INTEGER,
							pi_tano      IN VARCHAR2,
							pi_srcsys    IN VARCHAR2,
							pi_batchno   IN INTEGER) */

        //p_agrm_saration_oth(v_rt_othtran, v_closemths, pi_batchno)
        @SuppressWarnings("unchecked")
        List<Row> list_othagencytran = IteratorUtils.toList(itr_othagencytran);
        //othagencytran按日期排序
        Collections.sort(list_othagencytran, new Comparator<Row>() {
            public int compare(Row r1, Row r2) {
                return ((BigDecimal) r1.getAs("sk_confirmdate")).compareTo((BigDecimal) r2.getAs("sk_confirmdate"));
            }
        });
        itr_othagencytran = list_othagencytran.iterator();
        int protoCount = 0;
        @SuppressWarnings("unchecked")
        List<Row> ls = IteratorUtils.toList(itr_row_saration);
        List<Row> ls_saration = new ArrayList<>();

        for (int i = 0; i < ls.size(); ++i) {
            ls_saration.add(new GenericRowWithSchema(new Object[]{
                    ls.get(i).getAs("hashcode"),
                    ls.get(i).getAs("sk_agreement_of_ration"),
                    ls.get(i).getAs("sk_product"),
                    ls.get(i).getAs("dk_share_type"),
                    ls.get(i).getAs("sk_tradeacco_reg"),
                    ls.get(i).getAs("dk_tano"),
                    ls.get(i).getAs("sk_account_of_fd"),
                    ls.get(i).getAs("bk_fundaccount"),
                    ls.get(i).getAs("bk_tradeaccount"),
                    ls.get(i).getAs("agencyno"),
                    ls.get(i).getAs("netno"),
                    ls.get(i).getAs("sk_agency_of_open"),
                    ls.get(i).getAs("balance"),
                    ls.get(i).getAs("last_balance"),
                    ls.get(i).getAs("dk_ration_term"),
                    ls.get(i).getAs("delay"),
                    ls.get(i).getAs("allow_fail_times"),
                    ls.get(i).getAs("ration_date"),
                    ls.get(i).getAs("agio"),
                    ls.get(i).getAs("dk_ration_status"),
                    ls.get(i).getAs("open_date"),
                    ls.get(i).getAs("first_date"),
                    ls.get(i).getAs("last_date"),
                    ls.get(i).getAs("protocol_end_date"),
                    ls.get(i).getAs("close_date"),
                    ls.get(i).getAs("protocol_issue"),
                    ls.get(i).getAs("protocolno"),
                    ls.get(i).getAs("dk_is_valid"),
                    ls.get(i).getAs("sdata_serialno"),
                    ls.get(i).getAs("memo"),
                    ls.get(i).getAs("ddvc"),
                    ls.get(i).getAs("dk_system_of_upd"),
                    ls.get(i).getAs("batchno"),
                    ls.get(i).getAs("inserttime"),
                    ls.get(i).getAs("updatetime"),
                    ls.get(i).getAs("dk_system_of_sdata")
            }, agrm_saration_schema));
        }

        while (itr_othagencytran.hasNext()) {
            Row v_rt_othtran = itr_othagencytran.next();
            int look_i = -1;
            for (int i = 0; i < ls_saration.size(); ++i) {
                if (((BigDecimal) ls_saration.get(i).getAs("close_date")).compareTo(new BigDecimal("99991231")) == 0) {
                    look_i = i;
                    break; //应该只有一个99991231
                }
            }
            if (look_i == -1) {
                Row to_insert = p_ins_agrm_saration_oth(v_rt_othtran, pi_batchno, agrm_saration_schema, protoCount++);
                ls_saration.add(to_insert);
            } else {
                //如果已存在有效的定投协议，先判断当前协议是否需要销户
                Row row_saration = ls_saration.get(look_i);
                BigDecimal last_date = (BigDecimal) row_saration.getAs("last_date");
                BigDecimal v_shouldclosedate = getShouldCloseDate(last_date, final_v_closemths);

                //销户日期前扣款，更新最近扣款日期
                if (v_shouldclosedate.compareTo((BigDecimal) v_rt_othtran.getAs("sk_confirmdate")) >= 0) {
                    ls_saration.set(look_i, new GenericRowWithSchema(new Object[]{
                            row_saration.getAs("hashcode"),
                            row_saration.getAs("sk_agreement_of_ration"),
                            row_saration.getAs("sk_product"),
                            row_saration.getAs("dk_share_type"),
                            row_saration.getAs("sk_tradeacco_reg"),
                            row_saration.getAs("dk_tano"),
                            row_saration.getAs("sk_account_of_fd"),
                            row_saration.getAs("bk_fundaccount"),
                            row_saration.getAs("bk_tradeaccount"),
                            row_saration.getAs("agencyno"),
                            row_saration.getAs("netno"),
                            row_saration.getAs("sk_agency_of_open"),
                            row_saration.getAs("balance"),
                            v_rt_othtran.getAs("amount"), //last_balance
                            row_saration.getAs("dk_ration_term"),
                            row_saration.getAs("delay"),
                            row_saration.getAs("allow_fail_times"),
                            row_saration.getAs("ration_date"),
                            row_saration.getAs("agio"),
                            row_saration.getAs("dk_ration_status"),
                            row_saration.getAs("open_date"),
                            row_saration.getAs("first_date"),
                            v_rt_othtran.getAs("sk_confirmdate"), //last_date
                            row_saration.getAs("protocol_end_date"),
                            row_saration.getAs("close_date"),
                            row_saration.getAs("protocol_issue"),
                            row_saration.getAs("protocolno"),
                            row_saration.getAs("dk_is_valid"),
                            row_saration.getAs("sdata_serialno"),
                            row_saration.getAs("memo"),
                            row_saration.getAs("ddvc"),
                            row_saration.getAs("dk_system_of_upd"),
                            row_saration.getAs("batchno"),
                            row_saration.getAs("inserttime"),
                            new java.sql.Timestamp(System.currentTimeMillis()), //updatetime
                            row_saration.getAs("dk_system_of_sdata")
                    }, agrm_saration_schema));
                } else {
                    //销户日期前未发生过扣款，将当前协议销户，同时新增一个定投协议
                    //销户日期取当月最后一个工作日
                    BigDecimal v_closedate = getRealCloseDate(v_shouldclosedate);
                    //BigDecimal v_sk_agreement = row_saration.getAs("sk_agreement_of_ration");

                    ls_saration.set(look_i, new GenericRowWithSchema(new Object[]{
                            row_saration.getAs("hashcode"),
                            row_saration.getAs("sk_agreement_of_ration"),
                            row_saration.getAs("sk_product"),
                            row_saration.getAs("dk_share_type"),
                            row_saration.getAs("sk_tradeacco_reg"),
                            row_saration.getAs("dk_tano"),
                            row_saration.getAs("sk_account_of_fd"),
                            row_saration.getAs("bk_fundaccount"),
                            row_saration.getAs("bk_tradeaccount"),
                            row_saration.getAs("agencyno"),
                            row_saration.getAs("netno"),
                            row_saration.getAs("sk_agency_of_open"),
                            row_saration.getAs("balance"),
                            row_saration.getAs("last_balance"),
                            row_saration.getAs("dk_ration_term"),
                            row_saration.getAs("delay"),
                            row_saration.getAs("allow_fail_times"),
                            row_saration.getAs("ration_date"),
                            row_saration.getAs("agio"),
                            "0", //dk_ration_status
                            row_saration.getAs("open_date"),
                            row_saration.getAs("first_date"),
                            row_saration.getAs("last_date"),
                            row_saration.getAs("protocol_end_date"),
                            v_closedate, //close_date
                            row_saration.getAs("protocol_issue"),
                            row_saration.getAs("protocolno"),
                            row_saration.getAs("dk_is_valid"),
                            row_saration.getAs("sdata_serialno"),
                            row_saration.getAs("memo"),
                            row_saration.getAs("ddvc"),
                            row_saration.getAs("dk_system_of_upd"),
                            new BigDecimal(pi_batchno), //batchno
                            row_saration.getAs("inserttime"),
                            new java.sql.Timestamp(System.currentTimeMillis()), //updatetime
                            row_saration.getAs("dk_system_of_sdata")
                    }, agrm_saration_schema));

                    Row to_insert = p_ins_agrm_saration_oth(v_rt_othtran, pi_batchno, agrm_saration_schema, protoCount++);
                    ls_saration.add(to_insert);
                }//end of if 是否销户日期前扣款
            }//end of if 是否在ls_saration中存在99991231记录
        }//end of while
		
		/*
		    考虑数据重载，加载的时间段可能没有139扣款记录，在下面的语句里面关闭定投协议。
		    最近扣款日期距离加上三个月的之后所在月的最后一个工作日在加载的时间范围内。
	    */
        int look_i = -1;
        for (int i = 0; i < ls_saration.size(); ++i) {
            if (((BigDecimal) ls_saration.get(i).getAs("close_date")).compareTo(new BigDecimal("99991231")) == 0) {
                look_i = i;
                break;
            }
        }
        if (look_i != -1) {
            Row row_saration = ls_saration.get(look_i);
            BigDecimal last_date = (BigDecimal) row_saration.getAs("last_date");
            BigDecimal v_shouldclosedate = getShouldCloseDate(last_date, final_v_closemths);

            if (v_shouldclosedate.compareTo(new BigDecimal(pi_enddate)) <= 0) {
                BigDecimal v_closedate = getRealCloseDate(v_shouldclosedate);
                ls_saration.set(look_i, new GenericRowWithSchema(new Object[]{
                        row_saration.getAs("hashcode"),
                        row_saration.getAs("sk_agreement_of_ration"),
                        row_saration.getAs("sk_product"),
                        row_saration.getAs("dk_share_type"),
                        row_saration.getAs("sk_tradeacco_reg"),
                        row_saration.getAs("dk_tano"),
                        row_saration.getAs("sk_account_of_fd"),
                        row_saration.getAs("bk_fundaccount"),
                        row_saration.getAs("bk_tradeaccount"),
                        row_saration.getAs("agencyno"),
                        row_saration.getAs("netno"),
                        row_saration.getAs("sk_agency_of_open"),
                        row_saration.getAs("balance"),
                        row_saration.getAs("last_balance"),
                        row_saration.getAs("dk_ration_term"),
                        row_saration.getAs("delay"),
                        row_saration.getAs("allow_fail_times"),
                        row_saration.getAs("ration_date"),
                        row_saration.getAs("agio"),
                        "0", //dk_ration_status
                        row_saration.getAs("open_date"),
                        row_saration.getAs("first_date"),
                        row_saration.getAs("last_date"),
                        row_saration.getAs("protocol_end_date"),
                        v_closedate, //close_date
                        row_saration.getAs("protocol_issue"),
                        row_saration.getAs("protocolno"),
                        row_saration.getAs("dk_is_valid"),
                        row_saration.getAs("sdata_serialno"),
                        row_saration.getAs("memo"),
                        row_saration.getAs("ddvc"),
                        row_saration.getAs("dk_system_of_upd"),
                        new BigDecimal(pi_batchno), //batchno
                        row_saration.getAs("inserttime"),
                        new java.sql.Timestamp(System.currentTimeMillis()), //updatetime
                        row_saration.getAs("dk_system_of_sdata")
                }, agrm_saration_schema));
            }
        }
        return ls_saration;
    }

    public static BigDecimal getShouldCloseDate(BigDecimal day, Integer closemths) throws Exception {
        if (day == null) {
            return new BigDecimal("99991231");
        }
        Date dt = date_format.parse(day.toString());
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.MONTH, closemths.intValue()); //日期加3个月
        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH)); //获取当月最后一天
        return new BigDecimal(date_format.format(c.getTime()));
    }

    public static BigDecimal getRealCloseDate(BigDecimal shouldCloseDay) throws Exception {
        if (shouldCloseDay == null) {
            return new BigDecimal("99991231");
        }
        Date dt = date_format.parse(shouldCloseDay.toString());
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        if (c.get(Calendar.DAY_OF_WEEK) == 1) {
            //周日
            c.add(Calendar.DAY_OF_MONTH, -2);
            return new BigDecimal(date_format.format(c.getTime()));
        } else if (c.get(Calendar.DAY_OF_WEEK) == 7) {
            //周六
            c.add(Calendar.DAY_OF_MONTH, -1);
            return new BigDecimal(date_format.format(c.getTime()));
        } else {
            return new BigDecimal(date_format.format(c.getTime()));
        }
    }

    public static Row p_ins_agrm_saration_oth(Row pi_rt_othtran, String pi_batchno, StructType agrm_saration_schema, int protoCount) {
		/* 
		SELECT seq_sk_agreement.nextval INTO v_sk_agreement FROM dual; */
        BigDecimal v_sk_agreement = BigDecimal.valueOf(protoCount);
        return new GenericRowWithSchema(new Object[]{
                pi_rt_othtran.getAs("hashcode"),
                v_sk_agreement, //sk_agreement_of_ration
                pi_rt_othtran.getAs("sk_product"), //sk_product
                pi_rt_othtran.getAs("sharetype"), //dk_share_type
                pi_rt_othtran.getAs("sk_tradeaccount"), //sk_tradeacco_reg
                pi_rt_othtran.getAs("tano"), //dk_tano
                pi_rt_othtran.getAs("sk_fundaccount"), //sk_account_of_fd
                pi_rt_othtran.getAs("bk_fundaccount"), //bk_tradeaccount
                pi_rt_othtran.getAs("bk_tradeaccount"), //bk_tradeaccount
                pi_rt_othtran.getAs("agencyno"), //agencyno
                pi_rt_othtran.getAs("netno"), //netno
                pi_rt_othtran.getAs("sk_agency"), //sk_agency_of_open
                pi_rt_othtran.getAs("amount"), //balance
                pi_rt_othtran.getAs("amount"), //last_balance
                null, //dk_ration_term
                null, //delay
                null, //allow_fail_times
                BigDecimal.valueOf(((BigDecimal) pi_rt_othtran.getAs("sk_confirmdate")).intValue() % 100), //ration_date
                null, //agio
                "1", //dk_ration_status
                pi_rt_othtran.getAs("sk_confirmdate"), //open_date
                pi_rt_othtran.getAs("sk_confirmdate"), //first_date
                pi_rt_othtran.getAs("sk_confirmdate"), //last_date
                null, //protocol_end_date
                new BigDecimal("99991231"), //close_date
                null, //protocol_issue
                pi_rt_othtran.getAs("protocolno"), //protocolno
                "1", //dk_is_valid
                null, //sdata_serialno
                "初始新增（139）", //memo
                null, //ddvc
                pi_rt_othtran.getAs("srcsys"), //dk_system_of_upd
                new BigDecimal(pi_batchno), //batchno
                new java.sql.Timestamp(System.currentTimeMillis()), //inserttime
                new java.sql.Timestamp(System.currentTimeMillis()), //updatetime
                pi_rt_othtran.getAs("srcsys"), //dk_system_of_sdata
        }, agrm_saration_schema);
    }
}
