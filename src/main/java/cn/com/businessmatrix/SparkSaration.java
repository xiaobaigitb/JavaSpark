package cn.com.businessmatrix;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * 定投算法

 */
public class SparkSaration {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
    public static GregorianCalendar gc = new GregorianCalendar();
    public static List<String> issueList = Arrays.asList(new String[]{"36", "60", "90"});

    public static void main(String[] args) throws Exception {

        String srcsys = args[0];//系统
        String sparktable = args[1];//存放spark临时数据的表
        String startdate = args[2];
        String enddate = args[3];
        String schema = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String has159agency = args[6];//有开销协议的机构,逗号份额
        String closemths = args[7];//定投协议几个月没有定投记录就关闭一个协议
        String batchno = args[8];

        List<String> has159agencyList = new ArrayList(Arrays.asList(has159agency.split(",")));
        Properties properties = new Properties();
        String agrm_sarationTable = null;
        String tmp_ration_othagencytranTable = null;
        String tmp_ration_icbctranTable = null;
        String comm_cldr_customTable = null;
        String comm_bchmk_cldrTable = null;
        String tmp_ration_icbcdupopenTable = null;
        try {
            properties.load(SparkSaration.class.getResourceAsStream("/table.properties"));
            agrm_sarationTable = properties.getProperty("agrm_sarationTable");
            tmp_ration_othagencytranTable = properties.getProperty("tmp_ration_othagencytranTable");
            tmp_ration_icbctranTable = properties.getProperty("tmp_ration_icbctranTable");
            comm_cldr_customTable = properties.getProperty("comm_cldr_customTable");
            comm_bchmk_cldrTable = properties.getProperty("comm_bchmk_cldrTable");
            tmp_ration_icbcdupopenTable = properties.getProperty("tmp_ration_icbcdupopenTable");

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.dynamic.partition", "true")
                //.config("spark.sql.parquet.writeLegacyFormat", "true")
                .enableHiveSupport()
                .appName(sparkname)
                //.master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();

        hc.sql("alter table " + schema + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "')");
        System.out.println("alter table " + schema + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "')");

        String lastwkdate = "SELECT t2.dt_yearmth,t1.sk_date\n" +
                "FROM " + schema + "." + comm_cldr_customTable + " t1, " +
                schema + "." + comm_bchmk_cldrTable + " t2\n" +
                "WHERE t1.sk_date = t2.sk_date\n" +
                "AND t1.sk_calendar = 1\n" +
                "AND t1.dk_is_mth_last_workday = '1'\n" +
                " and t2.dt_yearmth between " + startdate.substring(0, 6) + " and  " + enddate.substring(0, 6);
        System.out.println(lastwkdate);
        System.out.println();

        List<Row> lastWkDateList = hc.sql(lastwkdate).collectAsList();
        Map<String, String> wkMap = new HashMap<String, String>();
        for (int i = 0; i < lastWkDateList.size(); i++) {
            Row r = lastWkDateList.get(i);
            wkMap.put(r.getAs(0).toString(), r.getAs(1).toString());
        }
        String sql_lookdf = "  select  concat_ws(','," +
                "cast(sk_account_of_fd as string)," +
                "cast(sk_tradeacco_reg as string)," +
                "cast(protocolno as string)," +
                "cast(dk_share_type as string)," +
                "cast(sk_product       as string))  as hashcode, " +
                "sk_agreement_of_ration,\n" +
                "sk_product,\n" +
                "dk_share_type,\n" +
                "sk_tradeacco_reg,\n" +
                "dk_tano,\n" +
                "sk_account_of_fd,\n" +
                "bk_fundaccount,\n" +
                "bk_tradeaccount,\n" +
                "agencyno,\n" +
                "netno,\n" +
                "sk_agency_of_open,\n" +
                "balance,\n" +
                "last_balance,\n" +
                "dk_ration_term,\n" +
                "delay,\n" +
                "allow_fail_times,\n" +
                "ration_date,\n" +
                "agio,\n" +
                "dk_ration_status,\n" +
                "open_date,\n" +
                "first_date,\n" +
                "last_date,\n" +
                "protocol_end_date,\n" +
                "close_date,\n" +
                "protocol_issue,\n" +
                "protocolno,\n" +
                "dk_is_valid,\n" +
                "sdata_serialno,\n" +
                "memo,\n" +
                "ddvc,\n" +
                "dk_system_of_upd,\n" +
                "batchno,\n" +
                "inserttime,\n" +
                "updatetime,\n" +
                "dk_system_of_sdata\n" +
                "  from " + schema + "." + agrm_sarationTable +
                " where dk_system_of_sdata='" + srcsys + "'" +
                " and close_date = 99991231 ";
        System.out.println(sql_lookdf);
        System.out.println();

        String sql_oth = " select  concat_ws(','," +
                "cast(sk_fundaccount as string)," +
                "cast(sk_tradeaccount as string)," +
                "cast(protocolno as string)," +
                "cast(sharetype as string)," +
                "cast(sk_product as string))  as hashcode, " +
                //"customerid,\n" +
                "sk_fundaccount,\n" +
                "sk_tradeaccount,\n" +
                "sk_agency,\n" +
                "bk_fundaccount,\n" +
                "bk_tradeaccount,\n" +
                "custtype,\n" +
                "agencyno,\n" +
                "netno,\n" +
                "tano,\n" +
                "sk_product,\n" +
                "bk_fundcode,\n" +
                "sharetype,\n" +
                "protocolno,\n" +
                "sk_confirmdate,\n" +
                "amount,\n" +
                //"shares,\n" +
                "srcsys,\n" +
                "batchno,\n" +
                //"inserttime,\n" +
                //"updatetime,\n" +
                "cserialno\n" +
                " from " + schema + "." + tmp_ration_othagencytranTable +
                " where srcsys='" + srcsys +
                "' and  sk_confirmdate BETWEEN " + startdate + " AND " + enddate;

        System.out.println("sql_oth=");
        System.out.println(sql_oth);
        System.out.println();

        String sql_icbc = "  select concat_ws(','," +
                "cast(a.sk_fundaccount as string)," +
                "cast(a.sk_tradeaccount as string)," +
                "cast(a.protocolno as string)," +
                "cast(a.sharetype as string)," +
                "cast(a.sk_product as string))  as hashcode, " +
                //"customerid,\n" +
                "a.sk_fundaccount,\n" +
                "a.sk_tradeaccount,\n" +
                "a.sk_agency,\n" +
                "a.bk_fundaccount,\n" +
                "a.bk_tradeaccount,\n" +
                "a.custtype,\n" +
                "a.agencyno,\n" +
                "a.netno,\n" +
                "a.tano,\n" +
                "a.sk_product,\n" +
                "a.bk_fundcode,\n" +
                "a.sharetype,\n" +
                "a.protocolno,\n" +
                "a.sk_confirmdate,\n" +
                "a.amount,\n" +
                //"shares,\n" +
                "a.requestamount,\n" +
                "a.bk_tradetype,\n" +
                "a.srcsys,\n" +
                "a.batchno,\n" +
                //"inserttime,\n" +
                //"updatetime,\n" +
                "a.cserialno\n" +
                " from       " + schema + "." + tmp_ration_icbctranTable + " a " +
                " left join " + schema + "." + tmp_ration_icbcdupopenTable + " b" +
                " ON a.sk_fundaccount = b.sk_fundaccount \n" +
                " AND a.sk_tradeaccount = b.sk_tradeaccount \n" +
                " AND a.sharetype = b.sharetype \n" +
                " AND a.agencyno = b.agencyno \n" +
                " AND a.tano = b.tano \n" +
                " AND a.srcsys = b.srcsys \n" +
                " AND a.protocolno = b.protocolno \n" +
                " AND a.sk_confirmdate = b.sk_confirmdate \n" +
                " AND a.sk_product = b.sk_product \n" +
                " where a.srcsys='" + srcsys + "'" +
                " and  a.sk_confirmdate BETWEEN " + startdate + " AND " + enddate +
                " and (a.bk_tradetype IN ('159', '139') OR  a.bk_tradetype = '160' AND b.sk_fundaccount IS NULL) ";

        System.out.println("sql_icbc=");
        System.out.println(sql_icbc);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        lookdf = lookdf.repartition(lookdf.col("hashcode"));

        //其他机构数据
        Dataset<Row> adf_oth = hc.sql(sql_oth);
        adf_oth = adf_oth.repartition(adf_oth.col("hashcode"));

        //工行数据
        Dataset<Row> adf_icbc = hc.sql(sql_icbc);
        adf_icbc = adf_icbc.repartition(adf_icbc.col("hashcode"));

        StructType st = lookdf.schema();
        lookdf.registerTempTable("tmp_register_table");
        //非工行数据
        String sql_filter = "select * from tmp_register_table where agencyno not in('" + StringUtils.join(has159agencyList, "','") + "')";
        System.out.println(sql_filter);
        System.out.println();
        JavaRDD<Iterable<Row>> w = handleNext_oth(adf_oth,
                hc.sql(sql_filter),
                //lookdf.filter(lookdf.col("agencyno").isin(has159agencyList)),
                st, closemths, wkMap, batchno, enddate);
        JavaRDD<Row> x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
            private static final long serialVersionUID = -6111647437563220114L;

            @Override
            public Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator();
            }
        });
        hc.createDataFrame(x, lookdf.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata")
                .mode(SaveMode.Append)
                .saveAsTable(schema + "." + sparktable);

        //工行数据
        sql_filter = "select * from tmp_register_table where agencyno  in('" + StringUtils.join(has159agencyList, "','") + "')";
        System.out.println(sql_filter);
        System.out.println();
        w = handleNext_icbc(adf_icbc, hc.sql(sql_filter), st, batchno);
        x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
            private static final long serialVersionUID = -6111647437563220114L;

            @Override
            public Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator();
            }
        });
        hc.createDataFrame(x, lookdf.schema()).write()
                .format("hive")
                .partitionBy("dk_system_of_sdata")
                .mode(SaveMode.Append)
                .saveAsTable(schema + "." + sparktable);
        hc.close();

    }


    static JavaRDD<Iterable<Row>> handleNext_oth(Dataset<Row> adf,
                                                 final Dataset<Row> lookdf,
                                                 StructType st,
                                                 String closemths,
                                                 Map<String, String> wkMap,
                                                 String batchno,
                                                 String enddate) {

        JavaPairRDD<Object, Row> ardd = adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();
        JavaPairRDD<Object, Row> lkprdd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> result = ardd.cogroup(lkprdd);

        JavaRDD<Iterable<Row>> w = result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {

            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                List<Row> ls = new ArrayList<Row>();
                Iterable<Row> tr = tuple._2._1;
                Iterable<Row> vr = tuple._2._2;

                Iterator<Row> itr = tr.iterator();
                Iterator<Row> ivr = vr.iterator();

                @SuppressWarnings("unchecked")
                List<Row> ltr = IteratorUtils.toList(itr);
                Collections.sort(ltr, new Comparator<Row>() {
                    public int compare(Row r1, Row r2) {
                        //按照日期排
                        int comp1 = ((BigDecimal) r1.getAs("sk_confirmdate")).compareTo((BigDecimal) r2.getAs("sk_confirmdate"));
                        String c1 = (String) r1.getAs("cserialno");
                        String c2 = (String) r2.getAs("cserialno");
                        int comp2 = c1.compareTo(c2);
                        int comp3 = -2;//按照流水号排序，但是数字格式来排序
                        if (StringUtils.isNumeric(c1) && StringUtils.isNumeric(c2)) {
                            comp3 = new BigDecimal(c1).compareTo(new BigDecimal(c2));
                        }
                        if (comp1 != 0)
                            return comp1;
                        else if (comp2 != 0)
                            return comp2;
                        else
                            return comp3;
                    }
                });

                //排序之后重新取iterator
                itr = ltr.iterator();

                Row tranRow = null;
                Row currRow = null;
                if (ivr.hasNext()) {
                    currRow = ivr.next();//基于前面的定投开始计算
                }

                while (itr.hasNext()) {
                    tranRow = itr.next();
                    if (currRow == null) {
                        //构造第一条出来
                        currRow = changeEt2_oth(tranRow, st, batchno);
                    } else {
                        currRow = changeEt_oth(currRow, tranRow, st, closemths, wkMap, batchno);
                        if ("0".equals(currRow.getAs("dk_ration_status"))) {
                            //上笔定投协议关闭了，重新构造一条
                            ls.add(currRow);
                            currRow = changeEt2_oth(tranRow, st, batchno);
                        }
                    }
                }//end while 交易
                //最后检查一下定投是否该关闭
                currRow = changeEt3_oth(currRow, st, batchno, closemths, wkMap, enddate);
                ls.add(currRow);
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

    static JavaRDD<Iterable<Row>> handleNext_icbc(Dataset<Row> adf,
                                                  final Dataset<Row> lookdf,
                                                  StructType st,
                                                  String batchno) {

        JavaPairRDD<Object, Row> ardd = adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();
        JavaPairRDD<Object, Row> lkprdd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc());//.cache();

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> result = ardd.cogroup(lkprdd);

        JavaRDD<Iterable<Row>> w = result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {

            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                List<Row> ls = new ArrayList<Row>();
                Iterable<Row> tr = tuple._2._1;
                Iterable<Row> vr = tuple._2._2;

                Iterator<Row> itr = tr.iterator();
                Iterator<Row> ivr = vr.iterator();

                @SuppressWarnings("unchecked")
                List<Row> ltr = IteratorUtils.toList(itr);
                Collections.sort(ltr, new Comparator<Row>() {
                    public int compare(Row r1, Row r2) {
                        //按照日期排
                        int comp1 = ((BigDecimal) r1.getAs("sk_confirmdate")).compareTo((BigDecimal) r2.getAs("sk_confirmdate"));
                        String c1 = (String) r1.getAs("cserialno");
                        String c2 = (String) r2.getAs("cserialno");

                        int comp2 = c1.compareTo(c2);
                        int comp3 = -2;//按照流水号排序，但是数字格式来排序
                        if (StringUtils.isNumeric(c1) && StringUtils.isNumeric(c2)) {
                            comp3 = new BigDecimal(c1).compareTo(new BigDecimal(c2));
                        }
                        int comp4 = decodeBkTradeType(r1.getAs("bk_tradetype")).compareTo(decodeBkTradeType(r2.getAs("bk_tradetype")));
                        if (comp1 != 0)
                            return comp1;
                        else if (comp4 != 0)
                            return comp4;
                        else if (comp2 != 0)
                            return comp2;
                        else
                            return comp3;
                    }
                });

                //排序之后重新取iterator
                itr = ltr.iterator();

                Row tranRow = null;
                Row currRow = null;
                if (ivr.hasNext()) {
                    currRow = ivr.next();//基于前面的定投开始计算
                }

                String bk_tradetype = null;
                while (itr.hasNext()) {
                    tranRow = itr.next();
                    bk_tradetype = tranRow.getAs("bk_tradetype");
                    if ("159".equals(bk_tradetype)) {
                        if (currRow == null) {
                            //前面没有协议，构造第一条出来
                            currRow = changeEt2_icbc(tranRow, st, batchno);
                        } else {
                            //前面有协议，关闭前面的，重新开一条。
                            String rationopendate = currRow.getAs("open_date").toString();
                            String sk_confirmdate = tranRow.getAs("sk_confirmdate").toString();
                            if (rationopendate.compareTo(sk_confirmdate) < 0) {
                                //关闭前面的
                                currRow = changeEt_icbc(currRow, tranRow, st, batchno);
                                ls.add(currRow);
                                //根据159新开一条
                                currRow = changeEt2_icbc(tranRow, st, batchno);
                            }
                        }
                    } else if ("160".equals(bk_tradetype)) {
                        if (currRow == null) {
                            //前面没有协议可以结束的
                        } else {
                            //销掉前面的
                            currRow = changeEt_icbc(currRow, tranRow, st, batchno);
                            ls.add(currRow);
                            currRow = null;//销掉之后，当前没有协议的
                        }
                    } else //139
                    {
                        if (currRow == null) {
                            //构造第一条出来
                            currRow = changeEt2_icbc(tranRow, st, batchno);
                        } else {
                            //根据139，修改前面的
                            currRow = changeEt3_icbc(currRow, tranRow, st, batchno);
                        }
                    }
                } //end while
                if (currRow != null)
                    ls.add(currRow);
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


    //有定投交易，
    static Row changeEt_oth(Row currRow, Row tranRow, StructType st, String closemths, Map<String, String> wkMap, String batchno) {
        //自然月最后一天
        String shouldclosedate = lastDayAfterAddMths(currRow.getAs("last_date").toString(), Integer.parseInt(closemths));
        String sk_confirmdate = tranRow.getAs("sk_confirmdate").toString();
        boolean isRationValid = (shouldclosedate.compareTo(sk_confirmdate) >= 0);//已有定投协议有效,因为在关闭前出现了交易

        String closedate = null;
        if (!isRationValid) {
            closedate = wkMap.get(shouldclosedate.substring(0, 6));
        }
        Row ret = new GenericRowWithSchema(new Object[]{
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("sk_agreement_of_ration")),//1
                currRow.getAs(st.fieldIndex("sk_product")),//2
                currRow.getAs(st.fieldIndex("dk_share_type")), //3
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //4
                currRow.getAs(st.fieldIndex("dk_tano")), //5
                currRow.getAs(st.fieldIndex("sk_account_of_fd")), //6
                currRow.getAs(st.fieldIndex("bk_fundaccount")),// 7
                currRow.getAs(st.fieldIndex("bk_tradeaccount")), //8
                currRow.getAs(st.fieldIndex("agencyno")), //9
                currRow.getAs(st.fieldIndex("netno")), //10
                currRow.getAs(st.fieldIndex("sk_agency_of_open")), //11
                currRow.getAs(st.fieldIndex("balance")),//12
                isRationValid ? tranRow.getAs("amount") : currRow.getAs(st.fieldIndex("last_balance")),//13
                currRow.getAs(st.fieldIndex("dk_ration_term")),//14
                currRow.getAs(st.fieldIndex("delay")), //15
                currRow.getAs(st.fieldIndex("allow_fail_times")), //16
                currRow.getAs(st.fieldIndex("ration_date")),  //17,
                currRow.getAs(st.fieldIndex("agio")),//18
                isRationValid ? currRow.getAs(st.fieldIndex("dk_ration_status")) : "0",//19
                currRow.getAs(st.fieldIndex("open_date")),//20
                currRow.getAs(st.fieldIndex("first_date")),//21
                isRationValid ? tranRow.getAs("sk_confirmdate") : currRow.getAs(st.fieldIndex("last_date")),//22
                currRow.getAs(st.fieldIndex("protocol_end_date")),//23
                isRationValid ? currRow.getAs(st.fieldIndex("close_date")) : new BigDecimal(closedate),//24
                currRow.getAs(st.fieldIndex("protocol_issue")),//25
                currRow.getAs(st.fieldIndex("protocolno")),//26
                currRow.getAs(st.fieldIndex("dk_is_valid")),//27
                currRow.getAs(st.fieldIndex("sdata_serialno")),//28
                "update",//29
                currRow.getAs(st.fieldIndex("ddvc")),//30
                currRow.getAs(st.fieldIndex("dk_system_of_upd")),//31
                new BigDecimal(batchno),//32
                currRow.getAs(st.fieldIndex("inserttime")), //33
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//35
        }, st);
        return ret;
    }

    static Row changeEt2_oth(Row tranRow, StructType st, String batchno) {
        Row ret = new GenericRowWithSchema(new Object[]{
                tranRow.getAs("hashcode"),        //0
                null,                                        //1 sk_agreement_of_ration
                tranRow.getAs("sk_product"),        //2
                tranRow.getAs("sharetype"),        //3
                tranRow.getAs("sk_tradeaccount"), //4
                tranRow.getAs("tano"),            //5
                tranRow.getAs("sk_fundaccount"),    //6
                tranRow.getAs("bk_fundaccount"),    //7
                tranRow.getAs("bk_tradeaccount"),  //8
                tranRow.getAs("agencyno"),        //9
                tranRow.getAs("netno"),            //10
                tranRow.getAs("sk_agency"),        //11-sk_agency_of_open
                tranRow.getAs("amount"),            //12-balance
                tranRow.getAs("amount"),            //13-last_balance
                null,                                        //14 dk_ration_term
                null,                                        //15 delay
                null,                                        //16 allow_fail_times
                new BigDecimal(Math.floorMod(((BigDecimal) tranRow.getAs("sk_confirmdate")).intValue(), 100)), // 17 ration_date
                null,                                        //18 agio
                "1",                                         //19 dk_ration_status
                tranRow.getAs("sk_confirmdate"),     //20 open_date
                tranRow.getAs("sk_confirmdate"),     //21 first_date
                tranRow.getAs("sk_confirmdate"),     //22 last_date
                null,                                         // 23 protocol_end_date
                new BigDecimal("99991231"),             //24 close_date
                null,                                         //25 protocol_issue
                tranRow.getAs("protocolno"),         //26 protocolno
                null,                                          //27 dk_is_valid
                null,                                          //28 sdata_serialno
                "new",                                          //29 memo
                null,                                           //30 ddvc
                tranRow.getAs("srcsys"),             //31 dk_system_of_upd
                new BigDecimal(batchno),//32
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //33 inserttime
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34 updatetime
                tranRow.getAs("srcsys")            //35 dk_system_of_sdata
        }, st);
        return ret;
    }

    //最后判断定投是否结束了
    static Row changeEt3_oth(Row currRow, StructType st, String batchno, String closemths, Map<String, String> wkMap, String enddate) {
        String shouldclosedate = lastDayAfterAddMths(currRow.getAs("last_date").toString(), Integer.parseInt(closemths));
        String closedate = wkMap.get(shouldclosedate.substring(0, 6));
        boolean isRationValid = false;

        if (closedate == null)  //说明关闭日期不在范围内
            isRationValid = true;
        else if (closedate.compareTo(enddate) <= 0) //该关闭了，而且这个时间都没有交易
            isRationValid = false;
        else isRationValid = true; //定投是否有效还不能下结论。

        Row ret = new GenericRowWithSchema(new Object[]{
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("sk_agreement_of_ration")),//1
                currRow.getAs(st.fieldIndex("sk_product")),//2
                currRow.getAs(st.fieldIndex("dk_share_type")), //3
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //4
                currRow.getAs(st.fieldIndex("dk_tano")), //5
                currRow.getAs(st.fieldIndex("sk_account_of_fd")), //6
                currRow.getAs(st.fieldIndex("bk_fundaccount")),// 7
                currRow.getAs(st.fieldIndex("bk_tradeaccount")), //8
                currRow.getAs(st.fieldIndex("agencyno")), //9
                currRow.getAs(st.fieldIndex("netno")), //10
                currRow.getAs(st.fieldIndex("sk_agency_of_open")), //11
                currRow.getAs(st.fieldIndex("balance")),//12
                currRow.getAs(st.fieldIndex("last_balance")),//13
                currRow.getAs(st.fieldIndex("dk_ration_term")),//14
                currRow.getAs(st.fieldIndex("delay")), //15
                currRow.getAs(st.fieldIndex("allow_fail_times")), //16
                currRow.getAs(st.fieldIndex("ration_date")),  //17,
                currRow.getAs(st.fieldIndex("agio")),//18
                isRationValid ? currRow.getAs(st.fieldIndex("dk_ration_status")) : "0",//19
                currRow.getAs(st.fieldIndex("open_date")),//20
                currRow.getAs(st.fieldIndex("first_date")),//21
                currRow.getAs(st.fieldIndex("last_date")),//22
                currRow.getAs(st.fieldIndex("protocol_end_date")),//23
                isRationValid ? currRow.getAs(st.fieldIndex("close_date")) : new BigDecimal(closedate),//24
                currRow.getAs(st.fieldIndex("protocol_issue")),//25
                currRow.getAs(st.fieldIndex("protocolno")),//26
                currRow.getAs(st.fieldIndex("dk_is_valid")),//27
                currRow.getAs(st.fieldIndex("sdata_serialno")),//28
                isRationValid ? currRow.getAs(st.fieldIndex("memo")) : "update-定投协议关闭了",//29
                currRow.getAs(st.fieldIndex("ddvc")),//30
                currRow.getAs(st.fieldIndex("dk_system_of_upd")),//31
                isRationValid ? currRow.getAs("batchno") : new BigDecimal(batchno),//32
                currRow.getAs(st.fieldIndex("inserttime")), //33
                isRationValid ? currRow.getAs(st.fieldIndex("updatetime")) : sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//35
        }, st);
        return ret;
    }

    static Row changeEt_icbc(Row currRow, Row tranRow, StructType st, String batchno) {
        String bk_tradetype = tranRow.getAs("bk_tradetype").toString();
        String sk_confirmdate = tranRow.getAs("sk_confirmdate").toString();
        String closedate = null;
        if ("159".equals(bk_tradetype)) {
            closedate = addDays(sk_confirmdate, -1);
        } else if ("160".equals(bk_tradetype)) {
            closedate = sk_confirmdate;
        }

        Row ret = new GenericRowWithSchema(new Object[]{
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("sk_agreement_of_ration")),//1
                currRow.getAs(st.fieldIndex("sk_product")),//2
                currRow.getAs(st.fieldIndex("dk_share_type")), //3
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //4
                currRow.getAs(st.fieldIndex("dk_tano")), //5
                currRow.getAs(st.fieldIndex("sk_account_of_fd")), //6
                currRow.getAs(st.fieldIndex("bk_fundaccount")),// 7
                currRow.getAs(st.fieldIndex("bk_tradeaccount")), //8
                currRow.getAs(st.fieldIndex("agencyno")), //9
                currRow.getAs(st.fieldIndex("netno")), //10
                currRow.getAs(st.fieldIndex("sk_agency_of_open")), //11
                currRow.getAs(st.fieldIndex("balance")),//12
                currRow.getAs(st.fieldIndex("last_balance")),//13
                currRow.getAs(st.fieldIndex("dk_ration_term")),//14
                currRow.getAs(st.fieldIndex("delay")), //15
                currRow.getAs(st.fieldIndex("allow_fail_times")), //16
                currRow.getAs(st.fieldIndex("ration_date")),  //17,
                currRow.getAs(st.fieldIndex("agio")),//18
                "0",//19
                currRow.getAs(st.fieldIndex("open_date")),//20
                currRow.getAs(st.fieldIndex("first_date")),//21
                currRow.getAs(st.fieldIndex("last_date")),//22
                currRow.getAs(st.fieldIndex("protocol_end_date")),//23
                new BigDecimal(closedate),//24
                currRow.getAs(st.fieldIndex("protocol_issue")),//25
                currRow.getAs(st.fieldIndex("protocolno")),//26
                currRow.getAs(st.fieldIndex("dk_is_valid")),//27
                currRow.getAs(st.fieldIndex("sdata_serialno")),//28
                "update",//29
                currRow.getAs(st.fieldIndex("ddvc")),//30
                currRow.getAs(st.fieldIndex("dk_system_of_upd")),//31
                new BigDecimal(batchno),//32
                currRow.getAs(st.fieldIndex("inserttime")), //33
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//35
        }, st);
        return ret;
    }

    static Row changeEt2_icbc(Row tranRow, StructType st, String batchno) {
        String bk_tradetype = tranRow.getAs("bk_tradetype");
        Row ret = new GenericRowWithSchema(new Object[]{
                tranRow.getAs("hashcode"),        //0
                null,                                        //1 sk_agreement_of_ration
                tranRow.getAs("sk_product"),        //2
                tranRow.getAs("sharetype"),        //3
                tranRow.getAs("sk_tradeaccount"), //4
                tranRow.getAs("tano"),            //5
                tranRow.getAs("sk_fundaccount"),    //6
                tranRow.getAs("bk_fundaccount"),    //7
                tranRow.getAs("bk_tradeaccount"),  //8
                tranRow.getAs("agencyno"),        //9
                tranRow.getAs("netno"),            //10
                tranRow.getAs("sk_agency"),        //11-sk_agency_of_open
                tranRow.getAs("requestamount"),    //12-balance
                "139".equals(bk_tradetype) ? tranRow.getAs("amount") : null,            //13-last_balance
                null,                                        //14 dk_ration_term
                null,                                        //15-delay
                null,                                        //16-allow_fail_times
                "139".equals(bk_tradetype) ? new BigDecimal(Math.floorMod(((BigDecimal) tranRow.getAs("sk_confirmdate")).intValue(), 100)) : null, // 17 ration_date
                null,                                        //18 agio
                "1",                                         //19 dk_ration_status
                tranRow.getAs("sk_confirmdate"),     //20 open_date
                "139".equals(bk_tradetype) ? tranRow.getAs("sk_confirmdate") : null,     //21 first_date
                "139".equals(bk_tradetype) ? tranRow.getAs("sk_confirmdate") : null,     //22 last_date
                new BigDecimal(getProtocolEndDate(tranRow)),     // 23 protocol_end_date
                new BigDecimal("99991231"),             //24 close_date
                new BigDecimal(getProtocolIssue(tranRow)),                                         //25 protocol_issue
                tranRow.getAs("protocolno"),         //26 protocolno
                null,                                          //27 dk_is_valid
                null,                                          //28 sdata_serialno
                "new",                                          //29 memo
                null,                                           //30 ddvc
                tranRow.getAs("srcsys"),             //31 dk_system_of_upd
                new BigDecimal(batchno),//32
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //33 inserttime
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34 updatetime
                tranRow.getAs("srcsys")            //35 dk_system_of_sdata
        }, st);
        return ret;
    }

    static Row changeEt3_icbc(Row currRow, Row tranRow, StructType st, String batchno) {
        Row ret = new GenericRowWithSchema(new Object[]{
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("sk_agreement_of_ration")),//1
                currRow.getAs(st.fieldIndex("sk_product")),//2
                currRow.getAs(st.fieldIndex("dk_share_type")), //3
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //4
                currRow.getAs(st.fieldIndex("dk_tano")), //5
                currRow.getAs(st.fieldIndex("sk_account_of_fd")), //6
                currRow.getAs(st.fieldIndex("bk_fundaccount")),// 7
                currRow.getAs(st.fieldIndex("bk_tradeaccount")), //8
                currRow.getAs(st.fieldIndex("agencyno")), //9
                currRow.getAs(st.fieldIndex("netno")), //10
                currRow.getAs(st.fieldIndex("sk_agency_of_open")), //11
                currRow.getAs(st.fieldIndex("balance")),//12
                tranRow.getAs("amount"),              //13 last_balance
                currRow.getAs(st.fieldIndex("dk_ration_term")),//14
                currRow.getAs(st.fieldIndex("delay")), //15
                currRow.getAs(st.fieldIndex("allow_fail_times")), //16
                //通过139过来开协议的
                currRow.getAs(st.fieldIndex("ration_date")) != null
                        ? currRow.getAs(st.fieldIndex("ration_date")) : new BigDecimal(Math.floorMod(((BigDecimal) tranRow.getAs("sk_confirmdate")).intValue(), 100)),  //17,
                currRow.getAs(st.fieldIndex("agio")),//18
                currRow.getAs(st.fieldIndex("dk_ration_status")),//19
                currRow.getAs(st.fieldIndex("open_date")),//20
                currRow.getAs(st.fieldIndex("first_date")) != null
                        ? currRow.getAs(st.fieldIndex("first_date")) : tranRow.getAs("sk_confirmdate"),//21
                tranRow.getAs("sk_confirmdate"),//22
                currRow.getAs(st.fieldIndex("protocol_end_date")),//23
                currRow.getAs(st.fieldIndex("close_date")),//24
                currRow.getAs(st.fieldIndex("protocol_issue")),//25
                currRow.getAs(st.fieldIndex("protocolno")),//26
                currRow.getAs(st.fieldIndex("dk_is_valid")),//27
                currRow.getAs(st.fieldIndex("sdata_serialno")),//28
                "update",//29
                currRow.getAs(st.fieldIndex("ddvc")),//30
                currRow.getAs(st.fieldIndex("dk_system_of_upd")),//31
                new BigDecimal(batchno),//32
                currRow.getAs(st.fieldIndex("inserttime")), //33
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //34
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//35
        }, st);
        return ret;
    }


    public static String lastDayAfterAddMths(String date, int mths) {
        try {
            gc.setTime(sdf2.parse(date));
            gc.add(Calendar.MONTH, mths);
            gc.set(Calendar.DAY_OF_MONTH, gc.getActualMaximum(Calendar.DAY_OF_MONTH));
            return sdf2.format(gc.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String addDays(String date, int days) {
        try {
            gc.setTime(sdf2.parse(date));
            gc.add(Calendar.DAY_OF_MONTH, days);
            return sdf2.format(gc.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getProtocolEndDate(Row tranRow) {
        String agencyno = tranRow.getAs("agencyno");
        String protocolno = tranRow.getAs("protocolno");
        if ("002".equals(agencyno)
                && issueList.contains(protocolno.substring(0, 2))
                && protocolno.length() >= 2
        ) {
            int mths = Integer.parseInt(protocolno.substring(0, 2));
            String sk_confirmdate = tranRow.getAs("sk_confirmdate").toString();
            String date = sk_confirmdate.substring(0, 6) + "01";
            try {
                gc.setTime(sdf2.parse(date));
                gc.add(Calendar.MONTH, mths);
                gc.set(Calendar.DAY_OF_MONTH, gc.getActualMaximum(Calendar.DAY_OF_MONTH));
                return sdf2.format(gc.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return "99991231";
    }

    static String getProtocolIssue(Row tranRow) {
        String agencyno = tranRow.getAs("agencyno");
        String protocolno = tranRow.getAs("protocolno");
        if ("002".equals(agencyno)
                && issueList.contains(protocolno.substring(0, 2))
                && protocolno.length() >= 2
        ) {
            return protocolno.substring(0, 2);
        } else {
            return "99";
        }
    }

    static String decodeBkTradeType(String bk_tradetype) {
        if (bk_tradetype.equals("159"))
            return "1";
        else if (bk_tradetype.equals("139"))
            return "2";
        else if (bk_tradetype.equals("160"))
            return "3";
        else
            return "0";
    }
}
