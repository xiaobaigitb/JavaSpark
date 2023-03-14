package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;

public class SparkRegionSplit {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .appName("SparkRegionSplit")
                //.master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();

        int sk_date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        String pi_srcsys = args[0];
        String pi_batchno = args[1];
        String pi_tgtTableName = args[2];
        String pi_schema = args[3];

        String sql_data = " SELECT sk_invpty, zipcode, contact_phone, mobile_phone " +
                ", dk_id_type,std_idno AS idno, address " +
                " FROM " + pi_schema + ".ip_invpty t " +
                " WHERE dk_system_of_upd = '" + pi_srcsys + "'" +
                " AND batchno = " + pi_batchno;

        System.out.println("================sql_data====================");
        System.out.println(sql_data);
        System.out.println("=================sql_data===================");
        Dataset<Row> ipdf = hc.sql(sql_data);

        String sql_rule = "SELECT t.rg_split_ruleno,t.dk_id_type"
                + ",t.rule_name" + ",t.dk_rg_split_method"
                + ",t.dk_rg_split_code" + ",t.rule_exp1" + ",t.priority"
                + " FROM " + pi_schema + ".comm_region_split_param t"
                + " WHERE t.dk_enabled_flag = 'Y'" + " ORDER BY t.priority";
        System.out.println("================sql_rule====================");
        System.out.println(sql_rule);
        System.out.println("=================sql_rule===================");

        Dataset<Row> rdf = hc.sql(sql_rule);

        String sql_mapp = "with x1 as\n"
                + " (SELECT DISTINCT t.dk_rg_split_code, t.bk_rg_split_code\n"
                + "    FROM " + pi_schema + ".comm_region_split_mapp t, " + pi_schema + ".comm_region_split_param t2\n"
                + "   WHERE t.dk_rg_split_code = t2.dk_rg_split_code\n"
                + "     AND t2.dk_rg_split_method = '1'),\n"
                + "x2 as\n"
                + " (\n"
                + "\n"
                + "  SELECT DISTINCT t.dk_rg_split_code AS dk_rg_split_code,\n"
                + "                   '*' AS bk_rg_split_code\n"
                + "    FROM " + pi_schema + ".comm_region_split_mapp t, " + pi_schema + ".comm_region_split_param t2\n"
                + "   WHERE t.dk_rg_split_code = t2.dk_rg_split_code\n"
                + "     AND t2.dk_rg_split_method <> '1')\n"
                + "select *\n"
                + "  from (select concat(x1.bk_rg_split_code, ',', x1.dk_rg_split_code) as key,\n"
                + "               t.sk_region,\n"
                + "               t.dk_rg_split_code,\n"
                + "               t.bk_rg_split_code,\n"
                + "               nvl(t2.effective_from, 19000101) AS effective_from,\n"
                + "               nvl(t2.effective_to, 99991231) AS effective_to,\n"
                + "               t2.sk_region_of_lv1 AS sk_region_of_lv1,\n"
                + "               t2.sk_region_of_lv2 AS sk_region_of_lv2, \n"
                + "               t3.bk_region,t2.hiear_level \n"
                + "          FROM " + pi_schema + ".comm_region_split_mapp t\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region_hierarchy t2\n"
                + "            ON t.sk_region = t2.sk_region\n"
                + "           AND t2.dk_region_tree = '01'\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region t3\n"
                + "            ON t2.sk_region_of_lv1 = t3.sk_region\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region t4\n"
                + "            ON t2.sk_region_of_lv2 = t4.sk_region\n"
                + "         INNER JOIN " + pi_schema + ".comm_region_split_param t5\n"
                + "            ON t.dk_rg_split_code = t5.dk_rg_split_code\n"
                + "         inner join x1\n"
                + "            on (t.bk_rg_split_code = x1.bk_rg_split_code AND\n"
                + "               t.dk_rg_split_code = x1.dk_rg_split_code)\n"
                + "        union all\n"
                + "\n"
                + "        select concat(x2.bk_rg_split_code, ',', x2.dk_rg_split_code) as key,\n"
                + "               t.sk_region,\n"
                + "               t.dk_rg_split_code,\n"
                + "               t.bk_rg_split_code,\n"
                + "               nvl(t2.effective_from, 19000101) AS effective_from,\n"
                + "               nvl(t2.effective_to, 99991231) AS effective_to,\n"
                + "               t2.sk_region_of_lv1 AS sk_region_of_lv1,\n"
                + "               t2.sk_region_of_lv2 AS sk_region_of_lv2, \n"
                + "               t3.bk_region,t2.hiear_level \n"
                + "          FROM " + pi_schema + ".comm_region_split_mapp t\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region_hierarchy t2\n"
                + "            ON t.sk_region = t2.sk_region\n"
                + "           AND t2.dk_region_tree = '01'\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region t3\n"
                + "            ON t2.sk_region_of_lv1 = t3.sk_region\n"
                + "          LEFT JOIN " + pi_schema + ".comm_region t4\n"
                + "            ON t2.sk_region_of_lv2 = t4.sk_region\n"
                + "         INNER JOIN " + pi_schema + ".comm_region_split_param t5\n"
                + "            ON t.dk_rg_split_code = t5.dk_rg_split_code\n"
                + "         inner join x2\n"
                + "            on t.dk_rg_split_code = x2.dk_rg_split_code\n"
                + "           AND t5.dk_rg_split_method <> '1') x\n"
                + " ORDER BY x.key, x.dk_rg_split_code\n"
                + " , x.hiear_level desc,x.bk_region ";
        System.out.println("================sql_mapp====================");
        System.out.println(sql_mapp);
        System.out.println("=================sql_mapp===================");
        Dataset<Row> ddf = hc.sql(sql_mapp);

        Row[] rules = (Row[]) rdf.collect();
        Row[] rs = (Row[]) ddf.coalesce(1).collect();
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
            Row r = ipdf.first();
            x(r, rules, sk_date, map);
            hc.close();
            return;
        }

        JavaRDD<Row> w = ipdf.toJavaRDD().map(new Function<Row, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row r) throws Exception {
                String rule1 = null;
                String dk_id_type = null;
                String field = null;
                String bk_rg_split_code = null;
                String bk_rg_split_code_src = null;
                int start = 0, len = 0;
                String[] splits = null;
                String[] ret = null;
                for (int i = 0; i < rules.length; i++) {
                    rule1 = rules[i].getAs("rule_exp1");
                    dk_id_type = rules[i].getAs("dk_id_type");
                    if (rule1 != null) {
                        splits = rule1.split(":", -1);
                        field = splits[0].toUpperCase();
                        if (splits.length == 1 || splits[1].length() == 0) {
                            start = 0;
                            len = rule1.length();
                        } else if (splits[1].length() > 0) {
                            splits = splits[1].split(",");
                            start = Integer.parseInt(splits[0]) - 1;
                            len = Integer.parseInt(splits[1]) + start;
                        }
                        if ("$$ZIPCODE".equals(field) && r.getAs("zipcode") != null)
                            bk_rg_split_code = r.getAs("zipcode").toString();
                        else if ("$$MOBILENO".equals(field) && r.getAs("mobile_phone") != null)
                            bk_rg_split_code = r.getAs("mobile_phone").toString();
                        else if ("$$TELNO".equals(field) && r.getAs("contact_phone") != null)
                            bk_rg_split_code = r.getAs("contact_phone").toString();
                        else if ("$$ADDRESS".equals(field) && r.getAs("address") != null)
                            bk_rg_split_code = r.getAs("address").toString();
                        else if ("$$IDNO".equals(field) && r.getAs("idno") != null
                                && r.getAs("dk_id_type") != null && r.getAs("dk_id_type").equals(dk_id_type)
                        )
                            bk_rg_split_code = r.getAs("idno").toString();
                        else
                            continue;

                        if (bk_rg_split_code != null && bk_rg_split_code.length() >= start) {
                            if (bk_rg_split_code.length() >= len)
                                bk_rg_split_code = bk_rg_split_code.substring(start, len);
                            else
                                bk_rg_split_code = bk_rg_split_code.substring(start);
                        }
                        if (bk_rg_split_code != null && bk_rg_split_code.length() > 0) {
                            bk_rg_split_code_src = bk_rg_split_code;
                            if ("ZIPCODE".equals(rules[i].getAs("dk_rg_split_code").toString()) && bk_rg_split_code_src.length() == 6) {
                                for (int y = 1; y <= 4; y++) {
                                    switch (y) {
                                        case 2:
                                            if (!"00".equals(bk_rg_split_code_src.substring(4, 6)))
                                                bk_rg_split_code = bk_rg_split_code_src.substring(0, 4) + "00";
                                            break;
                                        case 3:
                                            if (!"000".equals(bk_rg_split_code_src.substring(3, 6)))
                                                bk_rg_split_code = bk_rg_split_code_src.substring(0, 3) + "000";
                                            break;
                                        case 4:
                                            if (!"0000".equals(bk_rg_split_code_src.substring(2, 6)))
                                                bk_rg_split_code = bk_rg_split_code_src.substring(0, 2) + "0000";
                                            break;
                                    }
                                    ret = findRegion(sk_date, bk_rg_split_code, rules[i], map);
                                    if (ret != null) {
                                        break;
                                    }
                                }
                            } else {
                                ret = findRegion(sk_date, bk_rg_split_code, rules[i], map);
                            }

                            if (ret != null) {
                                break;
                            }
                        }
                    }
                }
                if (ret == null)
                    ret = new String[]{"-1", "-1"};
                return RowFactory.create(
                        r.getDecimal(0),
                        new BigDecimal(ret[0]),
                        ret[1]
                );
                //return r.getDecimal(0).toPlainString()+","+ret[0]+","+ret[1];//id,sk_region,ruleno
            }
        });//.saveAsTextFile(pi_file);
        StructType schema = hc.sql("select cast(0 as decimal(12,0)) as src_id"
                + ",cast(0 as decimal(12,0)) as sk_region,cast(null as string) as rule_id")
                .schema();
        hc.createDataFrame(w, schema).write().format("parquet").mode(SaveMode.Overwrite).saveAsTable(pi_tgtTableName);
        hc.close();
    }

    static String[] findRegion(int sk_date, String bk_rg_split_code, Row rule, Map<String, List<Row>> map) {
        String key = null;
        String dk_rg_split_method = rule.getAs("dk_rg_split_method");
        String dk_rg_split_code = rule.getAs("dk_rg_split_code");
        if (!"1".equals(dk_rg_split_method)) {
            key = "*," + dk_rg_split_code;
        } else {
            key = bk_rg_split_code + "," + dk_rg_split_code;
        }
        //System.out.println("key="+key);
        List<Row> ls = map.get(key);
        Row dr = null;
        String target = null;
        String[] ret = null;
        if (ls != null) {
            for (int i = 0; i < ls.size(); i++) {
                dr = ls.get(i);
                target = dr.getAs("bk_rg_split_code").toString();
                if (sk_date >= Integer.parseInt(dr.getAs("effective_from").toString())
                        && sk_date <= Integer.parseInt(dr.getAs("effective_to").toString())
                ) {
                    if ("0".equals(dk_rg_split_method)) {
                        if (!bk_rg_split_code.contains(target)) {
                            continue;
                        }

                    } else if ("2".equals(dk_rg_split_method)) {
                        if (!bk_rg_split_code.startsWith(target)) {
                            continue;
                        }
                    } else {
                        //System.out.println("bk_rg_split_code="+bk_rg_split_code+"-"+target);
                        if (!bk_rg_split_code.equals(target)) {
                            continue;
                        }
                    }
                    ret = new String[]{null, null};
                    //System.out.println("sk_region_of_lv2="+dr.getAs("sk_region_of_lv2"));
                    //System.out.println("sk_region_of_lv1="+dr.getAs("sk_region_of_lv1"));
                    if (dr.getAs("sk_region_of_lv2") == null)
                        if (dr.getAs("sk_region_of_lv1") == null)
                            ret[0] = "-1";
                        else
                            ret[0] = dr.getAs("sk_region_of_lv1").toString();
                    else
                        ret[0] = dr.getAs("sk_region_of_lv2").toString();
                    ret[1] = rule.getAs("rg_split_ruleno").toString();
                    //System.out.println("find="+true);
                    break;
                }
            }
        }
        return ret;
    }

    //用作本地调试
    public static void x(Row r, Row[] rules, int sk_date, Map<String, List<Row>> map) {
        String rule1 = null;
        String field = null;
        String bk_rg_split_code = null;
        String bk_rg_split_code_src = null;
        int start = 0, len = 0;
        String[] splits = null;
        String[] ret = null;
        for (int i = 0; i < rules.length; i++) {
            rule1 = rules[i].getAs("rule_exp1");
            if (rule1 != null) {
                splits = rule1.split(":", -1);
                field = splits[0].toUpperCase();
                if (splits.length == 1 || splits[1].length() == 0) {
                    start = 0;
                    len = rule1.length();
                } else if (splits[1].length() > 0) {
                    splits = splits[1].split(",");
                    start = Integer.parseInt(splits[0]) - 1;
                    len = Integer.parseInt(splits[1]);
                }
                if ("$$ZIPCODE".equals(field) && r.getAs("zipcode") != null)
                    bk_rg_split_code = r.getAs("zipcode").toString();
                else if ("$$MOBILENO".equals(field) && r.getAs("mobile_phone") != null)
                    bk_rg_split_code = r.getAs("mobile_phone").toString();
                else if ("$$TELNO".equals(field) && r.getAs("contact_phone") != null)
                    bk_rg_split_code = r.getAs("contact_phone").toString();
                else if ("$$ADDRESS".equals(field) && r.getAs("address") != null)
                    bk_rg_split_code = r.getAs("address").toString();
                else if ("$$IDNO".equals(field) && r.getAs("idno") != null)
                    bk_rg_split_code = r.getAs("idno").toString();
                else
                    continue;
                System.out.println("field=" + field);

                if (bk_rg_split_code != null && bk_rg_split_code.length() >= start) {
                    if (bk_rg_split_code.length() >= len)
                        bk_rg_split_code = bk_rg_split_code.substring(start, len);
                    else
                        bk_rg_split_code = bk_rg_split_code.substring(start);
                }
                if (bk_rg_split_code != null && bk_rg_split_code.length() > 0) {
                    bk_rg_split_code_src = bk_rg_split_code;
                    if ("ZIPCODE".equals(rules[i].getAs("dk_rg_split_code").toString())) {
                        for (int y = 1; y <= 4; y++) {
                            switch (y) {
                                case 2:
                                    if (!"00".equals(bk_rg_split_code_src.substring(4, 6)))
                                        bk_rg_split_code = bk_rg_split_code_src.substring(0, 4) + "00";
                                    break;
                                case 3:
                                    if (!"000".equals(bk_rg_split_code_src.substring(3, 6)))
                                        bk_rg_split_code = bk_rg_split_code_src.substring(0, 3) + "000";
                                    break;
                                case 4:
                                    if (!"0000".equals(bk_rg_split_code_src.substring(2, 6)))
                                        bk_rg_split_code = bk_rg_split_code_src.substring(0, 2) + "0000";
                                    break;
                            }
                            ret = findRegion(sk_date, bk_rg_split_code, rules[i], map);
                            if (ret != null) {
                                System.out.println("11111111111");
                                break;
                            }
                        }
                    } else {
                        ret = findRegion(sk_date, bk_rg_split_code, rules[i], map);
                    }

                    if (ret != null) {
                        System.out.println("22222222222222222");
                        break;
                    }
                }
            }
        }
        if (ret != null) {
            System.out.println("ret=" + r.getDecimal(0).toPlainString() + "," + ret[0] + "," + ret[1]);
        }
    }
}
