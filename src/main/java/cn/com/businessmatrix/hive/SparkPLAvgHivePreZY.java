package cn.com.businessmatrix.hive;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class SparkPLAvgHivePreZY {
    public static final double err = 0.000001d;
    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {


        String srcsys = args[0];//系统
        String sparktable = args[1];//存放spark临时数据的表
        String startdate = args[2];
        String enddate = args[3];
        String schema = args[4];
        String sparkname = args[5];//spark任务名称，方便查找
        String step = args[6];
        String midtable = args[7];
        String savemode = "append";
        if (args.length > 8)
            savemode = args[8];

        String transferInTypes = "(127,134)";//args[7];//转托管入，非交易过户入
        String transferOutTypes = "(128,135)";//args[8];//转托管出，非交易过户出
        List<String> transferInTypesList = new ArrayList(Arrays.asList(transferInTypes.replace("(", "").replace(")", "").split(",")));
        List<String> transferOutTypesList = new ArrayList(Arrays.asList(transferOutTypes.replace("(", "").replace(")", "").split(",")));
        List<String> transferTypesList = new ArrayList();
        transferTypesList.addAll(transferInTypesList);
        transferTypesList.addAll(transferOutTypesList);

        Properties properties = new Properties();
        String avgcostTable = null;
        String etTable = null;
        String inTable = null;
        String notinTable = null;
        String twdTable = null;
        try {
            properties.load(SparkPLAvgHiveZY.class.getResourceAsStream("/table.properties"));
            avgcostTable = properties.getProperty("avgcostTable");
            etTable = properties.getProperty("etTable");
            inTable = properties.getProperty("inTable");
            notinTable = properties.getProperty("notinTable");
            twdTable = properties.getProperty("twdTable");

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }


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

        hc.sql("alter table " + schema + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "')");
        System.out.println("alter table " + schema + "." + sparktable + " drop if exists partition(dk_system_of_sdata='" + srcsys + "')");

        {

            String sql_lookdf = null;

            sql_lookdf = "   select  concat_ws(',',"
                    + "cast(t1.sk_account_of_fd as string),"
                    + "cast(t1.sk_tradeacco_reg as string),"
                    + "cast(t1.sk_product       as string),t1.dk_share_type)  as hashcode, " + //0
                    "       t1.trd_it_trx_serialno," + //1
                    "       t1.dk_tano," + //2
                    "       t1.cserialno, " + //3
                    "       t1.requestno, " +  //4
                    "       t1.sk_invpty_of_cust, " +//5
                    "       t1.dk_cust_type, " + //6
                    "       t1.sk_account_of_fd, " +//7
                    "       t1.sk_tradeacco_reg, " + //8
                    "       t1.sk_currency, " +  //9
                    "       t1.agencyno, " + //10
                    "       t1.netno, " + //11
                    "       t1.bk_fundaccount," + //12
                    "       t1.bk_tradeaccount," +//13
                    "       t1.sk_product, " + //14
                    "       t1.dk_share_type, " + //15
                    "       t1.sk_agency, " + //16
                    "       t2.effective_to, " + //17
                    "       t1.net_value, " + //18
                    "       t1.sk_mkt_trade_type, " +//19
                    "       t1.dk_bourseflag, " + //20
                    "       t1.share_change, " + //21
                    "       t1.shares, " + //22
                    "       t1.avg_cost, " + //23
                    "       t1.divid_income, " + //24
                    "       t1.divid_income_out, " + //25
                    "       t1.total_income, " + //26
                    "       t1.total_income_chg, " + //26
                    "       t1.total_fee, " + //27
                    "       t1.total_cost," + //28
                    "       t1.total_cost_chg," + //28
                    "       t1.dk_is_valid, " + //29
                    "       t1.sdata_serialno, " + //30
                    "       t1.memo, " + //31
                    "       t1.ddvc, " + //32
                    "       t1.batchno, " + //33
                    "       t1.inserttime, " + //34
                    "       t1.updatetime, " + //35
                    "       t1.dk_system_of_upd, " +//36
                    "       t1.effective_from, " + //37
                    "       t1.dk_system_of_sdata " + //38
                    "  from " + schema + "." + avgcostTable + " t1 inner join " +
                    schema + "." + etTable + " t2 on t1.trd_it_trx_serialno=t2.trd_it_trx_serialno " +
                    " inner join   " + schema + "." + twdTable + " t3 on  t1.sk_account_of_fd = t3.sk_account_of_fd " +
                    " and t1.sk_tradeacco_reg = t3.sk_tradeacco_reg " +
                    " and t1.sk_product 	  = t3.sk_product " +
                    " and t1.dk_share_type    = t3.dk_share_type " +
                    " where t1.dk_system_of_sdata='" + srcsys + "'  and t2.effective_to=99991231 " +
                    " and t1.effective_from<=" + enddate +  //过滤掉不能参加扣减的数据
                    "  "
            ;

            System.out.println(sql_lookdf);
            System.out.println();


            String sql = "  select concat_ws(',',"
                    + "cast(t.sk_account_of_fd as string),"
                    + "cast(t.sk_tradeacco_reg as string),"
                    + "cast(t.sk_product       as string),t.dk_share_type)  as hashcode " +
                    "               ,t.sk_invpty_of_cust " +
                    "               ,t.dk_cust_type " +
                    "               ,t.sk_account_of_fd " +
                    "               ,t.sk_tradeacco_reg " +
                    "               ,t.sk_agency " +
                    "               ,t.sk_product " +
                    "               ,t.dk_share_type " +
                    "               ,t.sk_currency " +
                    "               ,t.sk_mkt_trade_type " +
                    "               ,t.cserialno " +
                    "               ,t.dk_tano " +
                    "               ,t.agencyno " +
                    "               ,t.netno " +
                    "               ,t.bk_fundaccount " +
                    "               ,t.bk_tradeaccount " +
                    "               ,t.confirm_balance " +
                    "               ,t.confirm_shares " +
                    "               ,t.total_fee " + //补差费从转出方转移到转入方
                    "               ,t.net_value " +
                    "               ,t.requestno " +
                    "               ,t.dk_bonus_type " +
                    "               ,t.dk_bourseflag" +
                    "               ,t.dk_system_of_sdata " +
                    "               ,t.ta_cfm_date " +
                    "               ,t.shr_chg_dir " +
                    "               ,t.income_rule " +
                    "               ,t.batchno " +
                    "               ,t.avg_cost " +
                    "               ,t.divid_income_out  from " + schema + "." + midtable + " t " +
                    " 		where t.dk_system_of_sdata='" + srcsys + "' ";

            System.out.println("sql=");
            System.out.println(sql);
            System.out.println();

            Dataset<Row> lookdf = hc.sql(sql_lookdf);

            Dataset<Row> adf = hc.sql(sql);

            StructType st = lookdf.schema();
            List<Row> w = handleNext2(adf, lookdf, st, transferInTypesList, transferOutTypesList, transferTypesList);


            System.out.println("===================================================================");
            System.out.println(schema + "." + sparktable);

            hc.createDataFrame(w, lookdf.schema()).write()
                    .format("hive")
                    .partitionBy("dk_system_of_sdata")
                    .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                    .saveAsTable(schema + "." + sparktable);
        } //end while

        //Thread.currentThread().sleep(1000*1000L);
        hc.close();

    }


    static List<Row> handleNext2(final Dataset<Row> adf,
                                 final Dataset<Row> lookdf, StructType st,
                                 final List<String> transferInTypesList,
                                 final List<String> transferOutTypesList,
                                 final List<String> transferTypesList) {

        List<Row> ls = new ArrayList<Row>();
        List<Row> lts = null;
        List<Row> lvs = null;

        List<Row> tr = adf.collectAsList();
        List<Row> vr = lookdf.collectAsList();
        Map<String, List<Row>> tmap = new HashMap<String, List<Row>>();
        Map<String, List<Row>> tmap2 = new HashMap<String, List<Row>>();
        Map<String, List<Row>> vmap = new HashMap<String, List<Row>>();

        Iterator<Row> itr = tr.iterator();
        Iterator<Row> ivr = vr.iterator();
        while (itr.hasNext()) {
            Row r = itr.next();
            String key = r.getAs("hashcode");
            lts = tmap.get(key);
            if (lts == null)
                lts = new ArrayList<Row>();
            lts.add(r);
            tmap.put(key, lts);
        }

        while (ivr.hasNext()) {
            Row r = ivr.next();
            String key = r.getAs("hashcode");
            lvs = vmap.get(key);
            if (lvs == null)
                lvs = new ArrayList<Row>();
            lvs.add(r);
            vmap.put(key, lvs);
        }

        Iterator<Entry<String, List<Row>>> z = tmap.entrySet().iterator();
        while (z.hasNext()) {
            Entry<String, List<Row>> m = z.next();
            itr = m.getValue().iterator();
            List<Row> ltr = IteratorUtils.toList(itr);
            Collections.sort(ltr, new Comparator<Row>() {
                public int compare(Row r1, Row r2) {
                    //按照日期排，分红排最前面,先份额减少，再份额增加，
                    int comp1 = ((BigDecimal) r1.getAs("ta_cfm_date")).compareTo((BigDecimal) r2.getAs("ta_cfm_date"));
                    int comp2 = 0;
                    int s1 = ((BigDecimal) r1.getAs("sk_mkt_trade_type")).intValue();
                    int s2 = ((BigDecimal) r2.getAs("sk_mkt_trade_type")).intValue();

                    //分红排前面，可能同一天分红和赎回
                    if (s1 == 143 && s2 != 143)
                        comp2 = -1;
                    else if (s1 != 143 && s2 == 143)
                        comp2 = 1;
                    else if (s1 == 128 && s2 == 127)
                        comp2 = -1;
                    else if (s1 == 127 && s2 == 128)
                        comp2 = 1;
                        //else if(s1==135&&s2==134)
                        //  comp2=-1;
                        //else if(s1==134&&s2==135)
                        //  comp2=1;
                    else if (s1 == 135 && s2 == 134)
                        comp2 = -1;
                    else if (s1 == 134 && s2 == 135)
                        comp2 = 1;
                    else comp2 = 0;

                    //先增加后减少份额
                    int comp3 = -((BigDecimal) r1.getAs("shr_chg_dir")).compareTo((BigDecimal) r2.getAs("shr_chg_dir"));
                    String c1 = (String) r1.getAs("cserialno");
                    String c2 = (String) r2.getAs("cserialno");
                    int comp4 = c1.compareTo(c2);
                    int comp5 = -2;//按照流水号排序，但是数字格式来排序
                    if (StringUtils.isNumeric(c1) && StringUtils.isNumeric(c2)) {
                        comp5 = new BigDecimal(c1).compareTo(new BigDecimal(c2));
                    }
                    if (comp1 != 0)
                        return comp1;
                    else if (comp2 != 0)
                        return comp2;
                    else if (comp3 != 0)
                        return comp3;
                    else {
                        if (comp5 != -2)
                            return comp5;
                        return comp4;
                    }
                }
            });
            tmap2.put(m.getKey(), ltr);
        }//先分组，再排序。

        z = tmap2.entrySet().iterator();
        List<Object[]> stack = new ArrayList<Object[]>();
        Map<String, Row> reqestnoMap = new HashMap<String, Row>();
        while (z.hasNext()) {
            Entry<String, List<Row>> m = z.next();
            itr = m.getValue().iterator();
            lvs = vmap.get(m.getKey());
            Row currRow = null;
            if (lvs != null && lvs.size() > 0)
                currRow = lvs.get(0);
            stack.add(new Object[]{itr, itr.next(), currRow, false});
        }

        //int count=0;
        while (!stack.isEmpty()) {
            //count++;
            doeach(stack, ls, st, transferInTypesList, transferOutTypesList, transferTypesList, reqestnoMap);
            //if(count>500)
            // break;
        }

        return ls;
    }

    static void doeach(List<Object[]> stack, List<Row> ls, StructType st,
                       final List<String> transferInTypesList,
                       final List<String> transferOutTypesList,
                       final List<String> transferTypesList,
                       Map<String, Row> reqestnoMap) {
        List<Object[]> removeList = new ArrayList<Object[]>();

        bkp:
//跳出到外循环点
        for (int s = 0; s < stack.size(); s++) {
            Object[] objs = stack.get(s);
            Iterator<Row> itr = (Iterator<Row>) objs[0];
            Row tranRow = (Row) objs[1];
            Row currRow = (Row) objs[2];
            Boolean isHandled = (Boolean) objs[3];//tranRow是否已经取出
            double[] d = new double[6];
            Row dpRow = null;

            //继续做下面的循环
            while (itr.hasNext() || !isHandled) {
                if (isHandled) //当前记录处理完成取下一条
                    tranRow = itr.next();
                //System.out.println("436:cserialno=:::::"+tranRow.getAs("cserialno"));
                if (transferInTypesList.contains(tranRow.getAs("sk_mkt_trade_type").toString())) {
                    dpRow = reqestnoMap.get(tranRow.getAs("ta_cfm_date").toString() + "." + tranRow.getAs("requestno").toString());
                    if (dpRow == null) {
                        //保护现场
                        //System.out.println("443:requestno=:::::"+tranRow.getAs("requestno"));
                        stack.set(s, new Object[]{itr, tranRow, currRow, false});
                        continue bkp;
                    }
                }

                if (currRow == null) {
                    //构造第一条出来
                    currRow = changeEt2(tranRow, st, d, transferTypesList, dpRow);
                } else {
                    currRow = changeEt(currRow, tranRow, st, d);
                    ls.add(currRow);
                    currRow = changeEt2(tranRow, st, d, transferTypesList, dpRow);
                }
                //用完了要置空
                dpRow = null;


                if (transferOutTypesList.contains(tranRow.getAs("sk_mkt_trade_type").toString())) {
                    reqestnoMap.put(tranRow.getAs("ta_cfm_date").toString() + "." + tranRow.getAs("requestno").toString(), currRow);
                }

                isHandled = true;//当前交易处理完成。下一条没有取出
            }

            ls.add(currRow);
            //中间没有跳出，则说明需要移除
            removeList.add(objs);
        }//end for

        Iterator<Object[]> rmItr = removeList.iterator();
        while (rmItr.hasNext()) {
            stack.remove(rmItr.next());
        }
    }


    //只是截断effective_to和更新updatetime，老的那条已经在表里面，
    static Row changeEt(Row currRow, Row tranRow, StructType st, double[] params) {
        params[0] = ((BigDecimal) currRow.getAs("shares")).doubleValue();
        params[1] = ((BigDecimal) currRow.getAs("avg_cost")).doubleValue();
        params[2] = ((BigDecimal) currRow.getAs("divid_income")).doubleValue();
        params[3] = ((BigDecimal) currRow.getAs("total_income")).doubleValue();
        params[4] = 0;
        params[5] = ((BigDecimal) currRow.getAs("total_cost")).doubleValue();

        Row ret = new GenericRowWithSchema(new Object[]{
                // return RowFactory.create(
                currRow.getAs(st.fieldIndex("hashcode")),// 0
                currRow.getAs(st.fieldIndex("trd_it_trx_serialno")),//1
                currRow.getAs(st.fieldIndex("dk_tano")),//2
                currRow.getAs(st.fieldIndex("cserialno")), //3
                currRow.getAs(st.fieldIndex("requestno")), //4
                currRow.getAs(st.fieldIndex("sk_invpty_of_cust")), //5
                currRow.getAs(st.fieldIndex("dk_cust_type")), //6
                currRow.getAs(st.fieldIndex("sk_account_of_fd")),// 7
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //8
                currRow.getAs(st.fieldIndex("sk_currency")), //9
                currRow.getAs(st.fieldIndex("agencyno")), //10
                currRow.getAs(st.fieldIndex("netno")), //11
                currRow.getAs(st.fieldIndex("bk_fundaccount")),//12
                currRow.getAs(st.fieldIndex("bk_tradeaccount")),//13
                currRow.getAs(st.fieldIndex("sk_product")),//14
                currRow.getAs(st.fieldIndex("dk_share_type")), //15
                currRow.getAs(st.fieldIndex("sk_agency")), //16
                tranRow.getAs("ta_cfm_date"), //effective_to //17
                currRow.getAs(st.fieldIndex("net_value")),//18
                currRow.getAs(st.fieldIndex("sk_mkt_trade_type")),//19
                currRow.getAs(st.fieldIndex("dk_bourseflag")),//20
                currRow.getAs(st.fieldIndex("share_change")),//21
                currRow.getAs(st.fieldIndex("shares")),//22
                currRow.getAs(st.fieldIndex("avg_cost")),//23
                currRow.getAs(st.fieldIndex("divid_income")),//24
                currRow.getAs(st.fieldIndex("divid_income_out")),//25
                currRow.getAs(st.fieldIndex("total_income")),//26
                currRow.getAs(st.fieldIndex("total_income_chg")),//27
                currRow.getAs(st.fieldIndex("total_fee")),//28
                currRow.getAs(st.fieldIndex("total_cost")),//29,累计投入成本
                currRow.getAs(st.fieldIndex("total_cost_chg")),//30
                currRow.getAs(st.fieldIndex("dk_is_valid")),//31
                currRow.getAs(st.fieldIndex("sdata_serialno")),//32
                "set effective_to=ta_cfm_date",//33
                currRow.getAs(st.fieldIndex("ddvc")),//34
                currRow.getAs(st.fieldIndex("batchno")),//35
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //36
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //37
                tranRow.getAs("dk_system_of_sdata"),//38
                currRow.getAs(st.fieldIndex("effective_from")),//39 effective_from
                currRow.getAs(st.fieldIndex("dk_system_of_sdata"))//40
        }, st);
        return ret;
    }

    static Row changeEt2(Row tranRow, StructType st, double[] params, List<String> transferTypesList, Row dpRow) {
        double shares = params[0];
        double avg_cost = params[1];
        double divid_income = params[2];
        double total_income = params[3];
        double zero_flag = params[4];//是否全部赎回,暂时没有用
        double total_cost = params[5];

        double new_shares = 0;
        double new_avg_cost = 0;
        double new_divid_income = 0;
        double new_total_income = 0;
        double new_total_cost = total_cost;
        double divid_income_out = 0;//托管转出带走的分红

        double netvalue = ((BigDecimal) tranRow.getAs("net_value")).doubleValue();
        double tr_shares = ((BigDecimal) tranRow.getAs("confirm_shares")).doubleValue();
        double tr_amount = ((BigDecimal) tranRow.getAs("confirm_balance")).doubleValue();

        double tr_avg_cost = (dpRow != null) ? ((BigDecimal) dpRow.getAs("avg_cost")).doubleValue() : 0.0d;
        double tr_divid_income_out = (dpRow != null) ? ((BigDecimal) dpRow.getAs("divid_income_out")).doubleValue() : 0.0d;

        double tr_total_fee = ((BigDecimal) tranRow.getAs("total_fee")).doubleValue();
        int incomerule = ((BigDecimal) tranRow.getAs("income_rule")).intValue();
        int shr_chg_dir = ((BigDecimal) tranRow.getAs("shr_chg_dir")).intValue();
        int sk_mkt_trade_type = ((BigDecimal) tranRow.getAs("sk_mkt_trade_type")).intValue();
        String dk_bonus_type = (String) tranRow.getAs("dk_bonus_type");
        //System.out.println("sk_mkt_trade_type="+sk_mkt_trade_type+",dk_bonus_type="+dk_bonus_type);
        //'交易增减标识；0-份额不增不减，1-份额增加，-1-份额减少',
        //'盈亏计算规则\;0-不计入盈亏,1-交易确认金额计入收益,2-根据净值计算确认金额计入收益,-1-交易确认金额计入成本,-2-根据净值计算确认金额计入收益',

        double tr_cost = 0;
        if (shr_chg_dir == 1)//增加份额
        {
            if (sk_mkt_trade_type == 143)//分红按增加份额处理，但是收益规则处理写死
            {
                //同一天有分红，也有赎回的可能吗
                if ("1".equals(dk_bonus_type))//现金分红
                {
                    new_shares = shares;
                    new_avg_cost = avg_cost;
                    new_divid_income = divid_income + tr_amount;
                    //System.out.println("new_divid_income="+new_divid_income);
                    new_total_income = total_income;
                } else//红利再投
                {
                    new_shares = shares + tr_shares;
                    tr_cost = tr_shares * netvalue;
                    new_avg_cost = (avg_cost * shares + tr_cost) / new_shares;
                    new_divid_income = divid_income;
                    new_total_income = total_income;
                }
            } else {
                if (transferTypesList.contains(String.valueOf(sk_mkt_trade_type)))//转托管的算法不同
                {
                    tr_cost = tr_shares * tr_avg_cost;
                } else if (incomerule == -1) {
                    tr_cost = tr_amount;
                } else if (incomerule == -2) {
                    tr_cost = tr_shares * netvalue;
                }
                new_shares = shares + tr_shares;
                new_avg_cost = (avg_cost * shares + tr_cost + (sk_mkt_trade_type == 137 ? tr_total_fee : 0)) / new_shares;//基金转换入，算上补差费用
                new_divid_income = divid_income + tr_divid_income_out;
                new_total_income = total_income;
                new_total_cost = new_total_cost + tr_cost + (sk_mkt_trade_type == 137 ? tr_total_fee : 0);
            }
        } else if (shr_chg_dir == -1)//减少份额
        {
            if (transferTypesList.contains(String.valueOf(sk_mkt_trade_type)))//转托管的算法不同
            {
                tr_cost = 0;//tr_shares*avg_cost;卖掉份额产生的收益
                //2021-7-15 转托管出时，累计成本同步扣减
                new_total_cost = new_total_cost - tr_shares * avg_cost;
                //2021-3-5 调整算法，转拖管出，不带走分红，直接变成收益。所以注释下面一行
                //divid_income_out=divid_income*(tr_shares/shares);//带走分红
            } else if (incomerule == 1) {
                tr_cost = tr_amount - tr_shares * avg_cost;
            } else if (incomerule == 2) {
                tr_cost = tr_shares * (netvalue - avg_cost);
            }

            if (shares - tr_shares < err)//份额清零
            {
                zero_flag = 1;
                new_divid_income = 0;
                new_total_income = total_income + (divid_income - divid_income_out) + tr_cost;
            } else {
                new_divid_income = divid_income * (1 - tr_shares / shares);
                new_total_income = total_income + (divid_income * (tr_shares / shares) - divid_income_out) + tr_cost;
            }
            new_shares = shares - tr_shares;
            new_avg_cost = avg_cost;
        }


        if ((!Double.isFinite(new_avg_cost)) || (Math.abs(new_avg_cost) > 100000)) //平均成本超100000为异常数据
        {
            new_avg_cost = 0;
        }
        if (!Double.isFinite(divid_income_out)) {
            divid_income_out = 0;
            //System.out.println("divid_income_out,sk_account_of_fd="+tranRow.getAs("sk_account_of_fd"));
        }
        if (!Double.isFinite(new_total_income)) {
            new_total_income = 0;
            //System.out.println("new_total_income,sk_account_of_fd="+tranRow.getAs("sk_account_of_fd"));
        }
        if (!Double.isFinite(tr_shares)) {
            tr_shares = 0;
            //System.out.println("tr_shares,sk_account_of_fd="+tranRow.getAs("sk_account_of_fd"));
        }

        params[0] = new_shares;
        params[1] = new_avg_cost;
        params[2] = new_divid_income;
        params[3] = new_total_income;
        params[4] = zero_flag;
        params[5] = new_total_cost;


        Row ret = new GenericRowWithSchema(new Object[]{
                //return RowFactory.create(
                tranRow.getAs("hashcode"),//0
                null,//1
                tranRow.getAs("dk_tano"),//2
                tranRow.getAs("cserialno"), //3
                tranRow.getAs("requestno"), //4
                tranRow.getAs("sk_invpty_of_cust"), //5
                tranRow.getAs("dk_cust_type"), //6
                tranRow.getAs("sk_account_of_fd"),// 7
                tranRow.getAs("sk_tradeacco_reg"), //8
                tranRow.getAs("sk_currency"), //9
                tranRow.getAs("agencyno"), //10
                tranRow.getAs("netno"), //11
                tranRow.getAs("bk_fundaccount"),//12
                tranRow.getAs("bk_tradeaccount"),//13
                tranRow.getAs("sk_product"),//14
                tranRow.getAs("dk_share_type"), //15
                tranRow.getAs("sk_agency"), //16
                new BigDecimal("99991231"), //effective_to 17
                tranRow.getAs("net_value"), //18
                tranRow.getAs("sk_mkt_trade_type"), //19
                tranRow.getAs("dk_bourseflag"), //20
                new BigDecimal(tr_shares * shr_chg_dir), //share_change 21
                new BigDecimal(new_shares), //shares 22 剩余份额
                new BigDecimal(Double.parseDouble(new DecimalFormat("#.00000000").format(new_avg_cost))), // 23
                new BigDecimal(new_divid_income), //income 24
                new BigDecimal(divid_income_out), //income 25
                new BigDecimal(new_total_income), //total_income 26
                new BigDecimal(new_total_income - total_income), //total_income_chg 27
                tranRow.getAs("total_fee"), //28
                new BigDecimal(new_total_cost), //total_cost 29
                new BigDecimal(new_total_cost - total_cost), //total_cost_chg 30
                null,//dk_is_valid 31
                null,//sdata_serialno 32
                "new",//memo 33
                null,//ddvc 34
                tranRow.getAs("batchno"), // 35
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //36
                sdf.format(new java.sql.Timestamp(System.currentTimeMillis())), //37
                tranRow.getAs("dk_system_of_sdata"),//38
                tranRow.getAs("ta_cfm_date"), //effective_from 39,
                tranRow.getAs("dk_system_of_sdata")//dk_system_of_sdata 40
        }, st
        );
        return ret;
    }
}
