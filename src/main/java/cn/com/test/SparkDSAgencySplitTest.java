package cn.com.test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;

import cn.com.businessmatrix.utils.SparkSplitUtils;

public class SparkDSAgencySplitTest {

    /*
     * 管理区划拆分---本地调试模式
     */

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.set("spark.master", "local");
        conf.setAppName("debugspark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Row> dataList = Arrays.asList(initData());

        Row[] agencys = new Row[3];

        Row[] rules = new Row[3];

        initRules(agencys);

        Map<String, List<Row>> map = new HashMap<String, List<Row>>();
        List<Row> ls = null;
        String key = null;
        for (int i = 0; i < agencys.length; i++) {
            key = agencys[i].getString(0);
            ls = map.get(key);
            if (ls == null)
                ls = new ArrayList<Row>();
            ls.add(agencys[i]);
            map.put(key, ls);
        }


        for (int i = 0; i < dataList.size(); i++) {
            Row r1 = dataList.get(i);
            Row rs = x(r1, map, rules);
        }


        //sc.parallelize(w).saveAsTextFile("/tmp/12346");
        sc.close();
    }

    static Row x(Row r, Map<String, List<Row>> map, Row[] rules) {
        BigDecimal rule_id = null;
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
            rule_id = (BigDecimal) rules[i].getAs("ag_split_ruleno");
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
        return RowFactory.create(
                r.getDecimal(0),
                sk_agency,
                rule_id
        );
        //return r.getDecimal(0).toPlainString()+","+sk_agency+","+rule_id;//id,sk_region,ruleno
    }


    static void initRules(Row[] r) {
        StructField[] sfs = new StructField[4];
        sfs[0] = new StructField("bk_agency", DataTypes.StringType, true, null);
        sfs[1] = new StructField("sk_agency", new DecimalType(12, 0), true, null);
        sfs[2] = new StructField("effective_from", new DecimalType(8, 0), true, null);
        sfs[3] = new StructField("effective_to", new DecimalType(8, 0), true, null);

        StructType st = new StructType(sfs);
        r[0] = SparkSplitUtils.gr(RowFactory.create(
                "234...1111",
                new BigDecimal(1),
                new BigDecimal(19000101),
                new BigDecimal(99991231)), st);

        r[1] = SparkSplitUtils.gr(RowFactory.create(
                "234...2222",
                new BigDecimal(2),
                new BigDecimal(19000101),
                new BigDecimal(99991231)), st);

        r[2] = SparkSplitUtils.gr(RowFactory.create(
                "234...3333",
                new BigDecimal(3),
                new BigDecimal(19000101),
                new BigDecimal(99991231)), st);
        r[3] = SparkSplitUtils.gr(RowFactory.create(
                "234...4444",
                new BigDecimal(4),
                new BigDecimal(19000101),
                new BigDecimal(99991231)), st);
    }

    static Row[] initData() {
        Row[] r = new Row[2];
        StructField[] sfs = new StructField[15];
        sfs[0] = new StructField("id", new DecimalType(12, 0), true, null);
        sfs[1] = new StructField("agencyno", DataTypes.StringType, true, null);
        sfs[2] = new StructField("requestno", DataTypes.StringType, true, null);
        sfs[3] = new StructField("areacode", DataTypes.StringType, true, null);
        sfs[4] = new StructField("netno", DataTypes.StringType, true, null);

        sfs[5] = new StructField("marketioflag", DataTypes.StringType, true, null);
        sfs[6] = new StructField("tradeaccount", DataTypes.StringType, true, null);
        sfs[7] = new StructField("bk_region_of_trd", DataTypes.StringType, true, null);
        sfs[8] = new StructField("sk_agency_of_trd", new DecimalType(12, 0), true, null);
        sfs[9] = new StructField("sk_agency_of_ds_acct", new DecimalType(12, 0), true, null);
        sfs[10] = new StructField("capitalmode", DataTypes.StringType, true, null);

        sfs[11] = new StructField("sk_date", new DecimalType(8, 0), true, null);
        sfs[12] = new StructField("open_date_of_trdacct", new DecimalType(8, 0), true, null);
        sfs[13] = new StructField("bk_region", DataTypes.StringType, true, null);
        sfs[14] = new StructField("ds_agencnyno", DataTypes.StringType, true, null);


        StructType st = new StructType(sfs);


        r[0] = SparkSplitUtils.gr(RowFactory.create(
                new BigDecimal(654321L),  //0 trd_st_trx_serialno
                "234", //1 sk_invpty_type
                null,//2 sk_invpty
                null,//2 sk_invpty
                "6666",
                "9",
                "33234234234234234234",
                null,
                null,
                null,
                "M",               //10 dk_tano
                new BigDecimal(20200210),//11 lval_ag
                null,
                null,
                "234"
        ), st);
        r[1] = SparkSplitUtils.gr(RowFactory.create(
                new BigDecimal(123456L),  //0 trd_st_trx_serialno
                "234", //1 sk_invpty_type
                null,//2 sk_invpty
                null,//2 sk_invpty
                "1111",
                "9",
                "33234234234234234234",
                null,
                null,
                null,
                "M",               //10 dk_tano
                new BigDecimal(20200210),//11 lval_ag
                null,
                null,
                "234"
        ), st);
		/*
		r[1]=SparkSplitUtils.gr(RowFactory.create( 				
				new BigDecimal(753862371L),  //0 trd_st_trx_serialno
				new BigDecimal(-1), //1 sk_invpty_type
				new BigDecimal(-1),//2 sk_invpty
				new BigDecimal(-1),//3 sk_account_type
				new BigDecimal(2),//4 sk_account				
				new BigDecimal(-1),//5 sk_product_type
				new BigDecimal(-1),//6 sk_product
				"*",			   //7 dk_agency_type
				new BigDecimal(-1),//8 sk_agency
				new BigDecimal(-1),//9 sk_region
				"*",			   //10 dk_tano
				new BigDecimal(10),//11 lval_ag
				new BigDecimal(100), //12 rval_ag
				new BigDecimal(10),//13 lval_rg
				new BigDecimal(100),//14 rval_rg
				new BigDecimal(0),//15 lval_ar 
				new BigDecimal(0), //16 rval_ar
				
				new BigDecimal(20200520),//17 sk_date
				new BigDecimal(-1),//18 sk_agency_of_lv1
				new BigDecimal(-1),//19 sk_region_of_lv1
				"*",  //20 cserialno
				"*",//21 dk_cust_type
				new BigDecimal(-1),//22 sk_tradeacco_reg
				"002",//23 agencyno
				"002",//24 netno
				"A"//25 dk_share_type
				),st); 
				*/
        return r;
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
