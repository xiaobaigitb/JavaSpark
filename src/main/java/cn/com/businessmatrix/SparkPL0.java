package cn.com.businessmatrix;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/*
 *  第一步骤：回滚数据
 *  参数顺序：
 *  	第一个参数：tano
 *  	第二个参数：batchno
 *  	第三个参数：startdate
 *      第四个参数：enddate
 *      第五个参数：schema
 *  要求：
 *   1.表fact_custincomechg_detail分区字段：tano,effective_from
 *   2.表fact_custincomechg_detail的字段顺序要按照代码的顺序。
 */
public class SparkPL0 {

    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {

        String tano = args[0];
        String batchno = args[1];
        String startdate = args[2];
        String enddate = args[3];
        String schema = args[4];

        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .config("hive.execution.engine", "spark")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.dynamic.partition", "true")
                .enableHiveSupport()
                .appName("SparkPL0")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();


        String sql =
                "insert overwrite table " + schema + ".tmp_fct_custincmchg_dtl_del " +
                        "  select a.tano, a.cserialno, a.oricserialno, a.effective_from " +
                        "    from test.fact_custincomechg_detail a " +
                        "   where a.effective_from >=" + startdate +
                        "     and a.tano = '" + tano + "'";

        System.out.println("================start====sql====1==============");
        System.out.println(sql);
        hc.sql(sql);
        System.out.println("================end====sql======1============");

        sql = "alter table " + schema + ".fact_custincomechg_detail drop if exists partition (tano='" + tano + "', effective_from>=" + startdate + ")";
        System.out.println("================start====sql====2==============");
        System.out.println(sql);
        hc.sql(sql);
        System.out.println("================end====sql======2============");

        sql =
                "insert overwrite table " + schema + ".fact_custincomechg_detail partition " +
                        "  (tano, effective_from) " +
                        "  select a.cserialno, " +
                        "         a.oricserialno, " +
                        "         a.customerid, " +
                        "         a.custtype, " +
                        "         a.sk_fundaccount, " +
                        "         a.sk_tradeaccount, " +
                        "         a.agencyno, " +
                        "         a.netno, " +
                        "         a.bk_fundcode, " +
                        "         a.sharetype, " +
                        "         a.sk_agency, " +
                        "         a.regdate, " +
                        "         case " +
                        "           when b.cserialno is not null then " +
                        "            cast(99991231 as decimal(8, 0)) " +
                        "           else " +
                        "            a.effective_to " +
                        "         end as effective_to, " +
                        "         a.orinetvalue, " +
                        "         a.orishares, " +
                        "         a.oricost, " +
                        "         a.lastshares, " +
                        "         a.shrchg, " +
                        "         a.shares, " +
                        "         a.shrchgofnocost, " +
                        "         a.sharesofnocost, " +
                        "         a.cost, " +
                        "         a.incmofnocost, " +
                        "         a.income, " +
                        "         a.costofincome, " +
                        "         a.totalcost, " +
                        "         a.totalincmofnocost, " +
                        "         a.totalincome, " +
                        "         a.totalcostofincome, " +
                        "         a.bourseflag, " +
                        "         a.srcsys, " +
                        "        cast(" + batchno + " as decimal(12, 0)) " +
                        "         a.inserttime, " +
                        "         case " +
                        "           when b.cserialno is not null then " +
                        "            current_timestamp() " +
                        "           else " +
                        "            a.updatetime " +
                        "         end as updatetime, " +
                        "         a.bk_tradetype, " +
                        "         a.ori_bk_tradetype, " +
                        "         a.tano, " +
                        "         a.effective_from " +
                        "    from  " + schema + ".fact_custincomechg_detail a " +
                        "    left join  " + schema + ".tmp_fct_custincmchg_dtl_del b " +
                        "      on a.oricserialno = b.oricserialno " +
                        "     and a.effective_to = b.effective_from " +
                        "     and a.tano = b.tano ";
        System.out.println("================start====sql====3==============");
        System.out.println(sql);
        hc.sql(sql);
        System.out.println("================end====sql======3============");
        hc.close();
    }
}
