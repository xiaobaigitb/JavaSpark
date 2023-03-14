package cn.com.businessmatrix;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/*
 * 回滚方法二：不同与PL0里面的方法。
 */
public class SparkPL3 {

    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {

        String tano = args[0];
        String startdate = args[2];

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(sc);
        hc.setConf("hive.execution.engine", "spark");
        hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
        hc.setConf("hive.exec.dynamic.partition", "true");

        String sql =
                "insert overwrite table test.fact_custincomechg_detail partition " +
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
                        "         a.batchno, " +
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
                        "    from test.fact_custincomechg_detail a " +
                        "    full join test.tmp_fct_custincmchg_dtl_del b " +
                        "      on a.oricserialno = b.oricserialno " +
                        "     and a.effective_to = b.effective_from " +
                        "     and a.tano = b.tano ";
        System.out.println(sql);
        hc.sql(sql);
        sc.close();
    }
}
