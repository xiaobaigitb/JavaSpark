package cn.com.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Test1 {

    static Row gr(Row r, StructType st) {
        // Seq 转 List
        // List<Column> list =
        // scala.collection.JavaConversions.seqAsJavaList(seq);
        // List 转 Seq
        // List<Column> list = new ArrayList<>();
        // list.add(new Column("columnA"));
        // Seq<Column> seq =
        // JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
        return new GenericRowWithSchema(scala.collection.JavaConversions
                .seqAsJavaList(r.toSeq()).toArray(), st);
    }

    public static void main(String[] args) {
        StructField[] sfs = new StructField[2];
        sfs[0] = new StructField("priority", new DecimalType(5, 0), true, null);
        sfs[1] = new StructField("sk_date", new StringType(), true, null);
        StructType st = new StructType(sfs);

        Map<String, List<Row>> map = new HashMap<String, List<Row>>();
        List<Row> ret = new ArrayList<Row>();

        ret.add(gr(RowFactory.create(new BigDecimal(1), "10"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(3), "30"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(5), "50"), st));

        map.put("a", ret);

        ret = new ArrayList<Row>();

        ret.add(gr(RowFactory.create(new BigDecimal(2), "2"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(4), "4"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(8), "8"), st));

        map.put("b", ret);

        ret = new ArrayList<Row>();
        ret.add(gr(RowFactory.create(new BigDecimal(3), "3"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(6), "6"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(9), "9"), st));

        map.put("c", ret);

        ret = new ArrayList<Row>();
        ret.add(gr(RowFactory.create(new BigDecimal(10), "10"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(11), "11"), st));
        ret.add(gr(RowFactory.create(new BigDecimal(12), "12"), st));

        map.put("d", ret);

        List<Row> x = reorder(map);

        Iterator<Row> it = x.iterator();
        while (it.hasNext()) {
            System.out.println(it.next().getAs("sk_date").toString());
        }

    }

    static List<Row> reorder(Map<String, List<Row>> map) {
        List<Row> vt_area_split_cmn = null;
        List<Row> vt_tmp_ar_splt_cmn = null;
        Iterator<String> it = map.keySet().iterator();
        int v_count2, v_cnt1, v_cnt2, v_cnt3;
        while (it.hasNext()) {
            // --取当前拆分规则集
            String itKey = it.next();
            List<Row> vt_cur_ar_splt_cmn = map.get(itKey);
            if (vt_cur_ar_splt_cmn == null) {
                System.out.println("vc_key===" + itKey);
                continue;
            }
            int v_count1 = vt_cur_ar_splt_cmn.size(); // --当前拆分规则集数量
            if (vt_area_split_cmn != null) {
                vt_tmp_ar_splt_cmn = new ArrayList<Row>();
                // --合并上条拆分规则集和当前拆分规则集
                v_count2 = vt_area_split_cmn.size(); // --上条拆分规则集数量
                v_cnt1 = 0;// --当前拆分规则集下标
                v_cnt2 = 0; // --上条拆分规则集下标
                v_cnt3 = 0; // --合并排序规则集下标
                // --对相邻拆分规则集（每个规则集均已按优先级排序）按优先级从高到低配对对比，保留高的规则
                while (v_cnt1 < v_count1 && v_cnt2 < v_count2) {
                    if (((BigDecimal) vt_cur_ar_splt_cmn.get(v_cnt1).getAs(
                            "priority"))
                            .compareTo(((BigDecimal) vt_area_split_cmn.get(
                                    v_cnt2).getAs("priority"))) < 0) {

                        vt_tmp_ar_splt_cmn.add(vt_cur_ar_splt_cmn.get(v_cnt1));
                        v_cnt1 = v_cnt1 + 1;
                    } else {
                        vt_tmp_ar_splt_cmn.add(vt_area_split_cmn.get(v_cnt2));
                        v_cnt2 = v_cnt2 + 1;
                    }
                    // --检查是否存在重载规则
                    v_cnt3 = v_cnt3 + 1;
                }

                // --当前拆分规则集剩余规则直接保留到合并规则集的尾部
                while (v_cnt1 < v_count1) {
                    vt_tmp_ar_splt_cmn.add(vt_cur_ar_splt_cmn.get(v_cnt1));
                    v_cnt3 = v_cnt3 + 1;
                    v_cnt1 = v_cnt1 + 1;
                }

                // --上次拆分规则集剩余规则直接保留到合并规则集的尾部
                while (v_cnt2 < v_count2) {
                    vt_tmp_ar_splt_cmn.add(vt_area_split_cmn.get(v_cnt2));
                    // --检查是否存在重载规则
                    v_cnt3 = v_cnt3 + 1;
                    v_cnt2 = v_cnt2 + 1;
                }

                // 复制合并排序规则集
                vt_area_split_cmn = vt_tmp_ar_splt_cmn;
            } else { // --直接复制第一个拆分规则集
                vt_area_split_cmn = vt_cur_ar_splt_cmn;
            }
        }// end while
        if (vt_area_split_cmn == null)
            vt_area_split_cmn = new ArrayList<Row>();
        return vt_area_split_cmn;
    }

}
