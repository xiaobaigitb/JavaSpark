package cn.com.businessmatrix;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/*
 *  第二步骤：把新增份额数据写入目标表
 *  参数顺序：
 *  	第一个参数：tano
 *  	第二个参数：batchno
 *  	第三个参数：startdate
 *      第四个参数：enddate
 *      第五个参数：schema
 *  要求：
 *   1.表tmp_fact_custincomechg_detail的数据由etl先生成好，排除货币交易数据。
 *   2.表tmp_fact_custincomechg_detail建议按照tano做分区，而且对应的startdate,enddate日期内的数据要齐全。
 *
 */
public class SparkPL1 {

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
                .appName("SparkPL1")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();

        String sql =
                "  select  " +
                        "       t.cserialno, " +
                        "       t.cserialno as oricserialno, " +
                        "       t.customerid, " +
                        "       t.custtype, " +
                        "       t.sk_fundaccount, " +
                        "       t.sk_tradeaccount, " +
                        "       t.agencyno, " +
                        "       t.netno, " +
                        "       t.bk_fundcode, " +
                        "       t.sharetype, " +
                        "       t.sk_agency, " +
                        "       t.sk_confirmdate as regdate, " +
                        "       cast(99991231 as decimal(8,0)) as effective_to, " +
                        "       t.netvalue as orinetvalue, " +
                        "       t.shares as orishares, " +
                        "       cast(0 as decimal(17,2)) as lastshares, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '0' then " +
                        "          0 " +
                        "         else " +
                        "          t.shares " +
                        "       end as shares, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '0' then " +
                        "          0 " +
                        "         else " +
                        "          t.shares " +
                        "       end as shrchg, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '0' then " +
                        "          0 " +
                        "         else " +
                        "          t.shares " +
                        "       end as shrchgofnocost, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '0' then " +
                        "          0 " +
                        "         else " +
                        "          t.shares " +
                        "       end as sharesofnocost, " +
                        "       t.bourseflag, " +
                        "       t.bk_tradetype, " +
                        "       t.bk_tradetype as ori_bk_tradetype, " +
                        "       t.srcsys, " +
                        "       t.batchno, " +
                        "       current_timestamp() as inserttime, " +
                        "       current_timestamp() as updatetime, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '1' then " +
                        "          t.amount " +
                        "         else " +
                        "          0 " +
                        "       end as income, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '1' then " +
                        "          t.amount " +
                        "         else " +
                        "          0 " +
                        "       end as totalincome, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '1' then " +
                        "          t.amount " +
                        "         else " +
                        "          0 " +
                        "       end as incmofnocost, " +
                        "       case " +
                        "         when t.bk_tradetype = '143' and t.bonustype = '1' then " +
                        "          t.amount " +
                        "         else " +
                        "          0 " +
                        "       end as totalincmofnocost, " +
                        "       cast(0 as decimal(17,2)) as totalcostofincome, " +
                        "       cast(0 as decimal(17,2)) as costofincome, " +
                        "       case " +
                        "         when t.incomerule = -1 then " +
                        "          t.amount " +
                        "         when t.incomerule = -2 then " +
                        "          t.shares * t.netvalue " +
                        "         else " +
                        "          0 " +
                        "       end as oricost, " +
                        "       case " +
                        "         when t.incomerule = -1 then " +
                        "          t.amount " +
                        "         when t.incomerule = -2 then " +
                        "          t.shares * t.netvalue " +
                        "         else " +
                        "          0 " +
                        "       end as cost, " +
                        "       case " +
                        "         when t.incomerule = -1 then " +
                        "          t.amount " +
                        "         when t.incomerule = -2 then " +
                        "          t.shares * t.netvalue " +
                        "         else " +
                        "          0 " +
                        "       end as totalcost,t.tano," +
                        "       t.sk_confirmdate as effective_from " +
                        "  from " + schema + ".tmp_fact_custincomechg_detail t " +
                        " where t.sk_confirmdate between " + startdate + " and " + enddate +
                        " and t.tano='" + tano + "' and t.incflag=1 ";

        System.out.println("================start====sql====1==============");
        System.out.println(sql);
        hc.sql(sql).registerTempTable("c");
        System.out.println("================end====sql======1============");


        sql = "insert overwrite table test.fact_custincomechg_detail partition(tano,effective_from) "
                + " SELECT cserialno, oricserialno, customerid, custtype, sk_fundaccount, sk_tradeaccount, agencyno, netno, bk_fundcode, sharetype, sk_agency, "
                + "regdate, effective_to, orinetvalue, orishares, oricost, lastshares, shrchg, shares, shrchgofnocost, sharesofnocost, cost, incmofnocost, income, costofincome, totalcost, totalincmofnocost, totalincome, totalcostofincome, bourseflag, srcsys, "
                + "batchno, inserttime, updatetime, bk_tradetype, ori_bk_tradetype, tano, effective_from FROM c";
        System.out.println("================start====sql====2==============");
        System.out.println(sql);
        hc.sql(sql);
        System.out.println("================end====sql======2============");

        hc.close();
    }
}
