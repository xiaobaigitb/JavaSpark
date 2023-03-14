package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;


public class SparkDSAgencySplit {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .appName("SparkDSAgencySplit")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();

        String pi_srcsys = args[0];
        String pi_batchno = args[1];
        String pi_dsagencyno = args[2];
        String pi_tgtTableName = args[3];
        String schema_ods = args[4]; //增加库名参数

        String sql_data =
                "SELECT sk_account_of_ds " +
                        "      ,agencyno  as agencyno" +
                        "      ,NULL  as requestno" +
                        "      ,NULL  as areacode" +
                        "      ,childnetno  as netno" +
                        "      ,'9' as marketioflag " +
                        "      ,bk_tradeaccount AS tradeaccount " +
                        "      ,NULL AS bk_region_of_trd " +
                        "      ,NULL AS sk_agency_of_trd " +
                        "      ,NULL AS sk_agency_of_ds_acct " +
                        "      ,dk_capitalmode as capitalmode " +
                        "      ,nvl(open_date,cast(19000101 as decimal(8,0))) AS sk_date " +
                        "      ,null AS open_date_of_trdacct " +
                        "      ,null as bk_region " +
                        "      ,'" + pi_dsagencyno + "' AS ds_agencyno " +
                        "  FROM " + schema_ods + ".agrm_tradeaccount_ds t " //+
                //" WHERE t.dk_system_of_sdata = '"+pi_srcsys+"' " +
                //"   AND t.batchno ="+pi_batchno
                ;
        System.out.println("================sql_data====================");
        System.out.println(sql_data);
        System.out.println("=================sql_data===================");
        Dataset<Row> ipdf = hc.sql(sql_data);

        String sql_rule = "select t1.ag_split_ruleno " +
                "              ,t1.agencyno " +
                "              ,trim(t1.rule_name) as rule_name " +
                "              ,trim(t1.rule_exp1) as rule_exp1 " +
                "              ,trim(t1.rule_exp2) as rule_exp2 " +
                "              ,trim(t1.rule_exp3) as rule_exp3 " +
                "              ,trim(t1.rule_exp4) as rule_exp4 " +
                "              ,trim(t1.rule_exp5) as rule_exp5 " +
                "              ,t1.priority " +
                "              ,t1.effective_from " +
                "              ,t1.effective_to " +
                "          from " + schema_ods + ".ip_agency_split_param t1 " +
                "         where t1.dk_enabled_flag = 'Y' order by t1.priority";
        System.out.println("================sql_rule====================");
        System.out.println(sql_rule);
        System.out.println("=================sql_rule===================");

        Dataset<Row> rdf = hc.sql(sql_rule);

        String sql_mapp = "select t.bk_agency, t.sk_agency, t.effective_from, t.effective_to from " + schema_ods + ".ip_agency_mapp t";
        System.out.println("================sql_mapp====================");
        System.out.println(sql_mapp);
        System.out.println("=================sql_mapp===================");
        Dataset<Row> ddf = hc.sql(sql_mapp);

        Row[] rules = (Row[]) rdf.orderBy("priority").collect();
        Row[] rs = (Row[]) ddf.collect();
        Map<String, List<Row>> map = new HashMap<String, List<Row>>();
        List<Row> ls = null;
        String key = null;
        for (int i = 0; i < rs.length; i++) {
            key = rs[i].getString(0);
            ls = map.get(key);
            if (ls == null)
                ls = new ArrayList<Row>();
            ls.add(rs[i]);
            map.put(key, ls);
        }

        int z = 1;
        if (z == 0) {
            Row r = (Row) ipdf.first();
            //testfunction()
            hc.close();
            return;
        }


        JavaRDD<Row> w = ipdf.toJavaRDD().map(new Function<Row, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row r) throws Exception {
                String rule_id = null;
                String rule1 = null, rule2 = null, rule3 = null, rule4 = null;
                String rulename = null;

                int sk_date = ((BigDecimal) r.getAs("sk_date")).intValue();
                BigDecimal open_date_of_trdacct = ((BigDecimal) r.getAs("open_date_of_trdacct"));
                BigDecimal sk_agency_of_trd = ((BigDecimal) r.getAs("sk_agency_of_trd"));
                BigDecimal sk_agency_of_ds_acct = ((BigDecimal) r.getAs("sk_agency_of_ds_acct"));
                BigDecimal sk_agency = null;

                String agencyno = r.getAs("agencyno");
                String requestno = r.getAs("requestno");
                String tradeaccount = r.getAs("tradeaccount");
                String bk_region = "N" + r.getAs("bk_region");
                String areacode = r.getAs("areacode");
                String netno = r.getAs("netno");
                String ds_agencyno = r.getAs("ds_agencyno");
                String capitalmode = r.getAs("capitalmode");

                String bk_region_of_trd = r.getAs("bk_region_of_trd");
                String marketioflag = r.getAs("marketioflag");

                String field = null;
                String bk_agency = null;
                String agcode1 = null, agcode2 = null, agcode3 = null, agcode4 = null;
                int start = 0, len = 0;
                String[] splits = null;

                for (int i = 0; i < rules.length; i++) {
                    if (
                            (!("*".equals(rules[i].getAs("agencyno"))))
                                    && (!(agencyno.equals(rules[i].getAs("agencyno"))))
                    )
                        continue;

                    rule_id = (String) rules[i].getAs("ag_split_ruleno");
                    rulename = rules[i].getAs("rule_name");
                    if ("T_L".equals(rulename)) {
                        if (sk_agency_of_trd != null) {
                            sk_agency = sk_agency_of_trd;
                            break;
                        } else
                            continue;
                    } else {
                        if ("C_L".equals(rulename)) {
                            if (bk_region_of_trd == null) {
                                continue;
                            } else {
                                bk_region = "N" + bk_region_of_trd;
                            }
                        } else if ("T".equals(rulename) && "004".equals(agencyno)) {
                            sk_date = open_date_of_trdacct.intValue();
                        } else if (!"9".equals(marketioflag) && !"*".equals(agencyno)) {
                            continue;
                        } else if ("9".equals(marketioflag) && "*".equals(agencyno) && "S".equals(rulename)) {
                            continue;
                        }

                        rule1 = rules[i].getAs("rule_exp1");
                        if (rule1 != null) {
                            splits = rule1.split(":", -1);
                            if (splits[0] == null)
                                continue;

                            field = splits[0].toLowerCase();
                            splits = splits[1].split(",");
                            start = Integer.parseInt(splits[0]) - 1;
                            len = Integer.parseInt(splits[1]) + start;

                            if ((!"$$".equals(field.substring(0, field.length() <= 2 ? field.length() : 2))) && field.length() >= start) {
                                agcode1 = field.substring(start, field.length() <= len ? field.length() : len);
                            } else if ("$$agencyno".equals(field) && agencyno != null && agencyno.length() >= start) {
                                agcode1 = agencyno.substring(start, agencyno.length() <= len ? agencyno.length() : len);
                            } else if ("$$requestno".equals(field) && requestno != null && requestno.length() >= start) {
                                agcode1 = requestno.substring(start, requestno.length() <= len ? requestno.length() : len);
                            } else if ("$$tradeaccount".equals(field) && tradeaccount != null && tradeaccount.length() >= start) {
                                agcode1 = tradeaccount.substring(start, tradeaccount.length() <= len ? tradeaccount.length() : len);
                            } else if ("$$areacode".equals(field) && areacode != null && areacode.length() >= start) {
                                agcode1 = areacode.substring(start, areacode.length() <= len ? areacode.length() : len);
                            } else if ("$$netno".equals(field) && netno != null && netno.length() >= start) {
                                agcode1 = netno.substring(start, netno.length() <= len ? netno.length() : len);
                            } else if ("$$bk_region".equals(field) && bk_region != null && bk_region.length() >= start) {
                                agcode1 = bk_region.substring(start, bk_region.length() <= len ? bk_region.length() : len);
                            } else {
                                continue;
                            }
                        } else {
                            agcode1 = "";
                        }//end rule1

                        rule2 = rules[i].getAs("rule_exp2");
                        if (rule2 != null) {
                            splits = rule2.split(":", -1);
                            if (splits[0] == null)
                                continue;

                            field = splits[0].toLowerCase();
                            splits = splits[1].split(",");
                            start = Integer.parseInt(splits[0]) - 1;
                            len = Integer.parseInt(splits[1]) + start;

                            if ((!"$$".equals(field.substring(0, field.length() <= 2 ? field.length() : 2))) && field.length() >= start) {
                                if (agencyno.equals(ds_agencyno) && capitalmode != null) {
                                    agcode2 = field.substring(start, field.length() <= len ? field.length() : len);
                                } else if (agencyno.equals(ds_agencyno) && capitalmode == null) {
                                    if (sk_agency_of_ds_acct != null) {
                                        sk_agency = sk_agency_of_ds_acct;
                                        break;
                                    } else
                                        continue;
                                } else if (!agencyno.equals(ds_agencyno)) {
                                    agcode2 = field.substring(start, field.length() <= len ? field.length() : len);
                                } else {
                                    continue;
                                }
                            } else if ("$$agencyno".equals(field) && agencyno != null && agencyno.length() >= start) {
                                agcode2 = agencyno.substring(start, agencyno.length() <= len ? agencyno.length() : len);
                            } else if ("$$requestno".equals(field) && requestno != null && requestno.length() >= start) {
                                agcode2 = requestno.substring(start, requestno.length() <= len ? requestno.length() : len);
                            } else if ("$$tradeaccount".equals(field) && tradeaccount != null && tradeaccount.length() >= start) {
                                agcode2 = tradeaccount.substring(start, tradeaccount.length() <= len ? tradeaccount.length() : len);
                            } else if ("$$areacode".equals(field) && areacode != null && areacode.length() >= start) {
                                agcode2 = areacode.substring(start, areacode.length() <= len ? areacode.length() : len);
                            } else if ("$$netno".equals(field) && netno != null && netno.length() >= start) {
                                agcode2 = netno.substring(start, netno.length() <= len ? netno.length() : len);
                            } else if ("$$bk_region".equals(field) && bk_region != null && bk_region.length() >= start) {
                                agcode2 = bk_region.substring(start, bk_region.length() <= len ? bk_region.length() : len);
                            } else if ("$$capitalmode".equals(field) && agencyno.equals(ds_agencyno) && capitalmode != null && capitalmode.length() >= start) {
                                agcode2 = capitalmode.substring(start, capitalmode.length() <= len ? capitalmode.length() : len);
                            } else {
                                continue;
                            }
                        } else {
                            agcode2 = "";
                        }//end rule2

                        rule3 = rules[i].getAs("rule_exp3");
                        if (rule3 != null) {
                            splits = rule3.split(":", -1);
                            if (splits[0] == null)
                                continue;

                            field = splits[0].toLowerCase();
                            splits = splits[1].split(",");
                            start = Integer.parseInt(splits[0]) - 1;
                            len = Integer.parseInt(splits[1]) + start;

                            if ((!"$$".equals(field.substring(0, field.length() <= 2 ? field.length() : 2))) && field.length() >= start) {
                                agcode3 = field.substring(start, field.length() <= len ? field.length() : len);
                            } else if ("$$agencyno".equals(field) && agencyno != null && agencyno.length() >= start) {
                                agcode3 = agencyno.substring(start, agencyno.length() <= len ? agencyno.length() : len);
                            } else if ("$$requestno".equals(field) && requestno != null && requestno.length() >= start) {
                                agcode3 = requestno.substring(start, requestno.length() <= len ? requestno.length() : len);
                            } else if ("$$tradeaccount".equals(field) && tradeaccount != null && tradeaccount.length() >= start) {
                                agcode3 = tradeaccount.substring(start, tradeaccount.length() <= len ? tradeaccount.length() : len);
                            } else if ("$$areacode".equals(field) && areacode != null && areacode.length() >= start) {
                                agcode3 = areacode.substring(start, areacode.length() <= len ? areacode.length() : len);
                            } else if ("$$netno".equals(field) && netno != null && netno.length() >= start) {
                                agcode3 = netno.substring(start, netno.length() <= len ? netno.length() : len);
                            } else if ("$$bk_region".equals(field) && bk_region != null && bk_region.length() >= start) {
                                agcode3 = bk_region.substring(start, bk_region.length() <= len ? bk_region.length() : len);
                            } else {
                                continue;
                            }
                        } else {
                            agcode3 = "";
                        }//end rule3

                        rule4 = rules[i].getAs("rule_exp4");
                        if (rule4 != null) {
                            splits = rule4.split(":", -1);
                            if (splits[0] == null)
                                continue;

                            field = splits[0].toLowerCase();
                            splits = splits[1].split(",");
                            start = Integer.parseInt(splits[0]) - 1;
                            len = Integer.parseInt(splits[1]) + start;

                            if ((!"$$".equals(field.substring(0, field.length() <= 2 ? field.length() : 2))) && field.length() >= start) {
                                agcode4 = field.substring(start, bk_region.length() <= len ? bk_region.length() : len);
                            } else if ("$$agencyno".equals(field) && agencyno != null && agencyno.length() >= start) {
                                agcode4 = agencyno.substring(start, agencyno.length() <= len ? agencyno.length() : len);
                            } else if ("$$requestno".equals(field) && requestno != null && requestno.length() >= start) {
                                agcode4 = requestno.substring(start, requestno.length() <= len ? requestno.length() : len);
                            } else if ("$$tradeaccount".equals(field) && tradeaccount != null && tradeaccount.length() >= start) {
                                agcode4 = tradeaccount.substring(start, tradeaccount.length() <= len ? tradeaccount.length() : len);
                            } else if ("$$areacode".equals(field) && areacode != null && areacode.length() >= start) {
                                agcode4 = areacode.substring(start, areacode.length() <= len ? areacode.length() : len);
                            } else if ("$$netno".equals(field) && netno != null && netno.length() >= start) {
                                agcode4 = netno.substring(start, netno.length() <= len ? netno.length() : len);
                            } else if ("$$bk_region".equals(field) && bk_region != null && bk_region.length() >= start) {
                                agcode4 = bk_region.substring(start, bk_region.length() <= len ? bk_region.length() : len);
                            } else {
                                continue;
                            }
                        } else {
                            agcode4 = "";
                        }//end rule4
                    }
                    bk_agency = f_gen_bk_agency(agcode1, agcode2, agcode3, agcode4);
                    sk_agency = findSkAgency(sk_date, bk_agency, map);
                    if (sk_agency != null)
                        break;
                }//end for
                if (sk_agency == null) {
                    bk_agency = f_gen_bk_agency(agencyno, "", "", "");
                    sk_agency = findSkAgency(sk_date, bk_agency, map);
                }

                return RowFactory.create(
                        r.getDecimal(0),
                        sk_agency,
                        rule_id
                );
                //return r.getDecimal(0).toPlainString()+","+sk_agency+","+rule_id;//id,sk_region,ruleno
            }
        });
        StructType schema = hc.sql("select cast(0 as decimal(12,0)) as src_id"
                + ",cast(0 as decimal(12,0)) as sk_agency,cast(null as string) as rule_id")
                .schema();
        hc.createDataFrame(w, schema).write().format("parquet").mode(SaveMode.Overwrite).saveAsTable(pi_tgtTableName);
        ;
        hc.close();
    }

    static String f_gen_bk_agency(String pi_agcode1, String pi_agcode2, String pi_agcode3, String pi_agcode4) {
        String v_sep = ".";
        return pi_agcode1 + v_sep + pi_agcode2 + v_sep + pi_agcode3 + v_sep + pi_agcode4;
    }


    static BigDecimal findSkAgency(int sk_date, String bk_agency, Map<String, List<Row>> map) {
        List<Row> ls = map.get(bk_agency);
        Row dr = null;
        BigDecimal sk_agency = null;
        if (ls != null) {
            for (int i = 0; i < ls.size(); i++) {
                dr = ls.get(i);
                if (sk_date >= Integer.parseInt(dr.getAs("effective_from").toString())
                        && sk_date <= Integer.parseInt(dr.getAs("effective_to").toString())
                ) {
                    sk_agency = (BigDecimal) dr.getAs("sk_agency");
                    break;
                }
            }
        }
        return sk_agency;
    }
}
