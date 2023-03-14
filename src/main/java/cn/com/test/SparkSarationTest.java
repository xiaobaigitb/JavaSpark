package cn.com.test;

import cn.com.businessmatrix.JavaPairFlatMapPartitionFunc;
import cn.com.businessmatrix.utils.SparkSplitUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * 定投算法

 */
public class SparkSarationTest {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
    public static GregorianCalendar gc = new GregorianCalendar();
    public static List<String> issueList = Arrays.asList(new String[]{"36", "60", "90"});
    static StructType sarationType;
    static StructType transType;

    static {
        StructField[] sfs = new StructField[36];
        sfs[0] = new StructField("hashcode", DataTypes.StringType, true, Metadata.empty());
        sfs[1] = new StructField("sk_agreement_of_ration", new DecimalType(12, 0), true, Metadata.empty());
        sfs[2] = new StructField("sk_product", new DecimalType(12, 0), true, Metadata.empty());
        sfs[3] = new StructField("dk_share_type", DataTypes.StringType, true, Metadata.empty());
        sfs[4] = new StructField("sk_tradeacco_reg", new DecimalType(12, 0), true, Metadata.empty());
        sfs[5] = new StructField("dk_tano", DataTypes.StringType, true, Metadata.empty());
        sfs[6] = new StructField("sk_account_of_fd", new DecimalType(12, 0), true, Metadata.empty());
        sfs[7] = new StructField("bk_fundaccount", DataTypes.StringType, true, Metadata.empty());
        sfs[8] = new StructField("bk_tradeaccount", DataTypes.StringType, true, Metadata.empty());
        sfs[9] = new StructField("agencyno", DataTypes.StringType, true, Metadata.empty());
        sfs[10] = new StructField("netno", DataTypes.StringType, true, Metadata.empty());
        sfs[11] = new StructField("sk_agency_of_open", new DecimalType(12, 0), true, Metadata.empty());
        sfs[12] = new StructField("balance", new DecimalType(12, 0), true, Metadata.empty());
        sfs[13] = new StructField("last_balance", new DecimalType(12, 0), true, Metadata.empty());
        sfs[14] = new StructField("dk_ration_term", DataTypes.StringType, true, Metadata.empty());
        sfs[15] = new StructField("delay", new DecimalType(8, 0), true, Metadata.empty());
        sfs[16] = new StructField("allow_fail_times", new DecimalType(8, 0), true, Metadata.empty());
        sfs[17] = new StructField("ration_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[18] = new StructField("agio", new DecimalType(6, 0), true, Metadata.empty());
        sfs[19] = new StructField("dk_ration_status", DataTypes.StringType, true, Metadata.empty());
        sfs[20] = new StructField("open_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[21] = new StructField("first_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[22] = new StructField("last_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[23] = new StructField("protocol_end_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[24] = new StructField("close_date", new DecimalType(8, 0), true, Metadata.empty());
        sfs[25] = new StructField("protocol_issue", new DecimalType(6, 0), true, Metadata.empty());
        sfs[26] = new StructField("protocolno", DataTypes.StringType, true, Metadata.empty());
        sfs[27] = new StructField("dk_is_valid", DataTypes.StringType, true, Metadata.empty());
        sfs[28] = new StructField("sdata_serialno", DataTypes.StringType, true, Metadata.empty());
        sfs[29] = new StructField("memo", DataTypes.StringType, true, Metadata.empty());
        sfs[30] = new StructField("ddvc", DataTypes.StringType, true, Metadata.empty());
        sfs[31] = new StructField("dk_system_of_upd", DataTypes.StringType, true, Metadata.empty());
        sfs[32] = new StructField("batchno", DataTypes.StringType, true, Metadata.empty());
        sfs[33] = new StructField("inserttime", DataTypes.StringType, true, Metadata.empty());
        sfs[34] = new StructField("updatetime", DataTypes.StringType, true, Metadata.empty());
        sfs[35] = new StructField("dk_system_of_sdata", DataTypes.StringType, true, Metadata.empty());

        sarationType = new StructType(sfs);

    }

    static {
        StructField[] sfs = new StructField[19];
        sfs[0] = new StructField("hashcode", DataTypes.StringType, true, Metadata.empty());
        sfs[1] = new StructField("sk_fundaccount", new DecimalType(12, 0), true, Metadata.empty());
        sfs[2] = new StructField("sk_tradeaccount", new DecimalType(12, 0), true, Metadata.empty());
        sfs[3] = new StructField("sk_agency", new DecimalType(12, 0), true, Metadata.empty());
        sfs[4] = new StructField("bk_fundaccount", DataTypes.StringType, true, Metadata.empty());
        sfs[5] = new StructField("bk_tradeaccount", DataTypes.StringType, true, Metadata.empty());
        sfs[6] = new StructField("custtype", DataTypes.StringType, true, Metadata.empty());
        sfs[7] = new StructField("agencyno", DataTypes.StringType, true, Metadata.empty());
        sfs[8] = new StructField("netno", DataTypes.StringType, true, Metadata.empty());
        sfs[9] = new StructField("tano", DataTypes.StringType, true, Metadata.empty());
        sfs[10] = new StructField("sk_product", new DecimalType(12, 0), true, Metadata.empty());
        sfs[11] = new StructField("bk_fundcode", DataTypes.StringType, true, Metadata.empty());
        sfs[12] = new StructField("sharetype", DataTypes.StringType, true, Metadata.empty());
        sfs[13] = new StructField("protocolno", DataTypes.StringType, true, Metadata.empty());
        sfs[14] = new StructField("sk_confirmdate", new DecimalType(8, 0), true, Metadata.empty());
        sfs[15] = new StructField("amount", new DecimalType(12, 0), true, Metadata.empty());
        sfs[16] = new StructField("srcsys", DataTypes.StringType, true, Metadata.empty());
        sfs[17] = new StructField("batchno", DataTypes.StringType, true, Metadata.empty());
        sfs[18] = new StructField("cserialno", DataTypes.StringType, true, Metadata.empty());
        transType = new StructType(sfs);
    }

    static List<Row> initWkdate() {
        StructField[] sfs = new StructField[2];
        sfs[0] = new StructField("dt_yearmth", new DecimalType(6, 0), true, null);
        sfs[1] = new StructField("sk_date", new DecimalType(8, 0), true, null);
        List<Row> list = new ArrayList<Row>();
        StructType st = new StructType(sfs);
        list.add(SparkSplitUtils.gr(RowFactory.create(
                new BigDecimal(202107),
                new BigDecimal(20210730)), st)
        );
        return list;
    }

    static List<Row> initSaration() {
        List<Row> list = new ArrayList<Row>();
        StructType st = sarationType;
        list.add(SparkSplitUtils.gr(RowFactory.create(
                "0",
                new BigDecimal(1), //sk_agreement_of_ration", new DecimalType(12,0),true,null);
                new BigDecimal(2),//sk_product", new DecimalType(12,0),true,null);
                "A",//dk_share_type", DataTypes.StringType,true,null);
                new BigDecimal(3),//sk_tradeacco_reg", new DecimalType(12,0),true,null);
                "98",//dk_tano", DataTypes.StringType,true,null);
                new BigDecimal(4),//sk_account_of_fd", new DecimalType(12,0),true,null);
                "5",//bk_fundaccount", DataTypes.StringType,true,null);
                "6",//bk_tradeaccount", DataTypes.StringType,true,null);
                "003",//agencyno", DataTypes.StringType,true,null);
                "003",//netno", DataTypes.StringType,true,null);
                new BigDecimal(5),//sk_agency_of_open", new DecimalType(12,0),true,null);
                new BigDecimal(100),//balance", new DecimalType(12,0),true,null);
                new BigDecimal(200),//last_balance", new DecimalType(12,0),true,null);
                "12",//dk_ration_term", DataTypes.StringType,true,null);
                new BigDecimal(6),//delay", new DecimalType(8,0),true,null);
                new BigDecimal(7),//allow_fail_times", new DecimalType(8,0),true,null);
                new BigDecimal(8),//ration_date", new DecimalType(8,0),true,null);
                null,//agio", new DecimalType(6,0),true,null);
                "1",//dk_ration_status", DataTypes.StringType,true,null);
                new BigDecimal(20200101),//open_date", new DecimalType(8,0),true,null);
                new BigDecimal(20200101),//first_date", new DecimalType(8,0),true,null);
                new BigDecimal(20210408),//last_date", new DecimalType(8,0),true,null);
                new BigDecimal(99991231),//protocol_end_date", new DecimalType(8,0),true,null);
                new BigDecimal(99991231),//close_date", new DecimalType(8,0),true,null);
                new BigDecimal(9),//protocol_issue", new DecimalType(6,0),true,null);
                "-1",//protocolno", DataTypes.StringType,true,null);
                "1",//dk_is_valid", DataTypes.StringType,true,null);
                "1",//sdata_serialno", DataTypes.StringType,true,null);
                "1",//memo", DataTypes.StringType,true,null);
                "1",//ddvc", DataTypes.StringType,true,null);
                "lof",//dk_system_of_upd", DataTypes.StringType,true,null);
                "123",//batchno", DataTypes.StringType,true,null);
                "2021-1-1",//inserttime", DataTypes.StringType,true,null);
                "2021-1-1",//updatetime", DataTypes.StringType,true,null);
                "lof" //dk_system_of_sdata", DataTypes.StringType,true,null);
                ), st)
        );
        return list;
    }

    static List<Row> initTrans() {
        List<Row> list = new ArrayList<Row>();
        StructType st = transType;
        list.add(SparkSplitUtils.gr(RowFactory.create(
                "0",//hashcode", DataTypes.StringType,true,null);
                new BigDecimal(4),//sk_fundaccount", new DecimalType(12,0),true,null);
                new BigDecimal(3),//sk_tradeaccount", new DecimalType(12,0),true,null);
                new BigDecimal(5),//sk_agency", new DecimalType(12,0),true,null);
                "5",//bk_fundaccount", DataTypes.StringType,true,null);
                "6",//bk_tradeaccount", DataTypes.StringType,true,null);
                "1",//custtype", DataTypes.StringType,true,null);
                "003",//agencyno", DataTypes.StringType,true,null);
                "003",//netno", DataTypes.StringType,true,null);
                "98",//tano", DataTypes.StringType,true,null);
                new BigDecimal(2),//sk_product", new DecimalType(12,0),true,null);
                "000002",//bk_fundcode", DataTypes.StringType,true,null);
                "A",//sharetype", DataTypes.StringType,true,null);
                "-1",//protocolno", DataTypes.StringType,true,null);
                new BigDecimal(20210730),//sk_confirmdate", new DecimalType(8,0),true,null);
                new BigDecimal(10000),//amount", new DecimalType(12,0),true,null);
                "lof",//srcsys", DataTypes.StringType,true,null);
                "123",//batchno", DataTypes.StringType,true,null);
                "xxxxxxxxxx"//cserialno", DataTypes.StringType,true,null);
                ), st)
        );
        return list;
    }

    public static void main(String[] args) throws Exception {

        String closemths = "3";//定投协议几个月没有定投记录就关闭一个协议
        String batchno = "1";
        SparkConf conf = new SparkConf();
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .config("spark.testing.memory", (512 * 1014 * 1024) + "")
                .appName("sparkname")
                .master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();

        List<Row> lastWkDateList = initWkdate();
        Map<String, String> wkMap = new HashMap<String, String>();
        for (int i = 0; i < lastWkDateList.size(); i++) {
            Row r = lastWkDateList.get(i);
            wkMap.put(r.getAs(0).toString(), r.getAs(1).toString());
        }


        Dataset<Row> lookdf = hc.createDataFrame(initSaration(), sarationType);
        Dataset<Row> adf_oth = hc.createDataFrame(initTrans(), transType);
        StructType st = lookdf.schema();
        JavaRDD<Iterable<Row>> w = handleNext_oth(adf_oth,
                lookdf,
                st, closemths, wkMap, batchno);
        JavaRDD<Row> x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
            private static final long serialVersionUID = -6111647437563220114L;

            @Override
            public Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator();
            }
        });

        List<Row> y = x.collect();
        System.out.println(y);
        //hc.createDataFrame(x, lookdf.schema()).write().format("parquet")
        //		.save("D:\\bigdata\\tmp\\12346");
        hc.close();

    }


    static JavaRDD<Iterable<Row>> handleNext_oth(final Dataset<Row> adf,
                                                 final Dataset<Row> lookdf,
                                                 StructType st,
                                                 String closemths,
                                                 Map<String, String> wkMap,
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
                currRow = changeEt3_oth(currRow, st, batchno, closemths, wkMap);
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
        String shouldclosedate = lastDayAfterAddMths(currRow.getAs("last_date").toString(), Integer.parseInt(closemths));
        String sk_confirmdate = tranRow.getAs("sk_confirmdate").toString();
        boolean isRationValid = (shouldclosedate.compareTo(sk_confirmdate) >= 0);//已有定投协议有效
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
    static Row changeEt3_oth(Row currRow, StructType st, String batchno, String closemths, Map<String, String> wkMap) {
        String shouldclosedate = lastDayAfterAddMths(currRow.getAs("last_date").toString(), Integer.parseInt(closemths));
        String closedate = wkMap.get(shouldclosedate.substring(0, 6));//3个月后的最后一个工作日，不止3个月
        boolean isRationValid = (closedate == null);//说明能找到关闭日期

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
