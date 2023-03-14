package cn.com.test;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.com.businessmatrix.JavaPairFlatMapPartitionFunc;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.BoxedUnit;

/*
 *  第三步骤：对份额减少情况进行处理
 *  参数顺序：
 *  	第一个参数：tano
 *  	第二个参数：batchno
 *  	第三个参数：startdate
 *      第四个参数：enddate
 *      第五个参数：schema
 *      第六个参数：step
 *  要求：
 *   1.表。
 *
 */
public class SparkPL2Test {

    public static long val = 0;
    public static List<Long[]> list = null;
    public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {


        Map<String, String> dBConOption = new HashMap<String, String>();
        dBConOption.put("url", "jdbc:oracle:thin:@cdh08:1521:dw");
        dBConOption.put("user", "ods");
        dBConOption.put("password", "ods");
        dBConOption.put("driver", "oracle.jdbc.driver.OracleDriver");
        dBConOption.put("dbtable", "ass_ta_cust_income_detail");

        SparkConf conf = new SparkConf();
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hc = new HiveContext(sc);
        SparkSession hc = SparkSession.builder()
                .config(conf)
                .appName("SparkPL2")
                .master("local[4]")
                //.master("yarn-cluster")
                .getOrCreate();
        DataFrameReader dfRead = hc.read().format("jdbc").options(dBConOption);

        //dd.write().mode(SaveMode.Overwrite).save("/tmp/123.txt");
        //System.out.println(dd.count());
        // dd.printSchema();


        String srcsys = "LOFTA";
        String batchno = "1";
        String startdate = "20050101";
        String enddate = "20081231";
        String schema = "ods";
        int step = 30000;

        Date d_startdate = sdf.parse(startdate);
        Date d_enddate = sdf.parse(enddate);
        Date d_sp_startdate = d_startdate;
        Date d_sp_enddate = null;
        //dd.sqlContext().sql(sqlText)
        String sql_lookdf =
                "(select  hashcode, " +
                        "       trd_it_trx_serialno," +
                        "       dk_tano," +
                        "       cserialno, " +
                        "       ori_cserialno, " +
                        "       sk_invpty_of_cust, " +
                        "       dk_cust_type, " +
                        "       sk_account_of_fd, " +
                        "       sk_tradeacco_reg, " +
                        "       sk_currency, " +
                        "       agencyno, " +
                        "       netno, " +
                        "       sk_product, " +
                        "       dk_share_type, " +
                        "       sk_agency, " +
                        "       reg_date, " +
                        "       effective_to, " +
                        "       ori_net_value, " +
                        "       ori_sk_mkt_trade_type, " +
                        "       sk_mkt_trade_type, " +
                        "       dk_bourseflag, " +
                        "       ori_shares, " +
                        "       ori_cost, " +
                        "       last_shares, " +
                        "       share_change, " +
                        "       shares, " +
                        "       cost, " +
                        "       income, " +
                        "       income_incld_cost, " +
                        "       total_cost, " +
                        "       total_income, " +
                        "       total_income_incld_cost, " +
                        "       back_fee, " +
                        "       dk_is_valid, " +
                        "       dk_system_of_sdata, " +
                        "       sdata_serialno, " +
                        "       memo, " +
                        "       ddvc, " +
                        "       batchno, " +
                        "       inserttime, " +
                        "       updatetime, " +
                        "       dk_system_of_upd, " +
                        "       effective_from " +
                        "  from " + schema + ".ass_ta_cust_income_detail t " +
                        "  where t.dk_system_of_upd='" + srcsys + "' and t.shares>0 and t.effective_to=99991231) as tt "
                //" distribute by hashcode "+
                ;
        System.out.println(sql_lookdf);

        Dataset<Row> lookdf = dfRead.option("dbquery", sql_lookdf).load();
        lookdf = lookdf//.filter(lookdf.col("dk_system_of_upd").equalTo(srcsys))
                .repartition(lookdf.col("hashcode"))
                .sort(lookdf.col("reg_date").asc(), lookdf.col("ori_cserialno").asc());


        StructType st = lookdf.schema();
        //System.out.println("================end====sql_lookdf======1============");

        //if(d_sp_startdate.after(d_enddate))
        //break;
        d_sp_enddate = add(d_sp_startdate, step);
        if (d_sp_enddate.after(d_enddate))
            d_sp_enddate = d_enddate;

        String sql =
                "(select   hashcode " +
                        "               ,t.dk_tano " +
                        "               ,t.cserialno " +
                        "               ,t.sk_invpty_of_cust " +
                        "               ,t.dk_cust_type " +
                        "               ,t.sk_account_of_fd " +
                        "               ,t.sk_tradeacco_reg " +
                        "               ,t.sk_currency " +
                        "               ,t.agencyno " +
                        "               ,t.netno " +
                        "               ,t.sk_product " +
                        "               ,t.dk_share_type " +
                        "               ,t.dk_bonus_type " +
                        "               ,t.sk_agency " +
                        "               ,t.ta_cfm_date " +
                        "               ,t.confirm_shares " +
                        "               ,t.dk_bourseflag" +
                        "               ,t.confirm_balance " +
                        "               ,t.net_value " +
                        "               ,t.shr_chg_dir " +
                        "               ,t.sk_mkt_trade_type " +
                        "               ,t.income_rule " +
                        "               ,t.dk_system_of_upd " +
                        "               ,t.batchno " +
                        "  from " + schema + ".vw_trd_ta_saletran t " +
                        " where t.ta_cfm_date between " + sdf.format(d_sp_startdate) + " and " + sdf.format(d_sp_enddate) +
                        " and t.dk_system_of_upd='" + srcsys + "' and t.shr_chg_dir = -1 )  tt";

        //System.out.println("================start====sql====2==============");
        System.out.println(sql);
        Dataset<Row> adf = dfRead.option("dbtable", sql).load();
        adf = adf//.filter(adf.col("ta_cfm_date").between(sdf.format(d_sp_startdate), sdf.format(d_sp_enddate)))
                .repartition(adf.col("hashcode"))
                .sort(adf.col("ta_cfm_date").asc(), adf.col("cserialno").asc());
        System.out.println();
        adf.printSchema();
        System.out.println();

        JavaRDD<Iterable<Row>> w = handleNext2(adf, lookdf, st);
        JavaRDD<Row> x = w.flatMap(new FlatMapFunction<Iterable<Row>, Row>() {
            private static final long serialVersionUID = -6111647437563220114L;

            @Override
            public java.util.Iterator<Row> call(Iterable<Row> arg0) throws Exception {
                return arg0.iterator();
            }

        });


        List<Row> mm = x.collect();
        for (int i = 0; i < mm.size(); i++) {
            System.out.println(mm.get(i).toSeq().toString());
        }

        if (1 == 1)
            return;

        //System.out.println(x.count());
        //hc.sql("truncate table "+schema+".mid_fact_custincomechg_detail");
        hc.createDataFrame(x, lookdf.schema()).registerTempTable("c");
        hc.sql("insert into table  " + schema + ".mid_ass_ta_cust_income_detail  select * from c");
        //.write().format("parquet").mode(SaveMode.Overwrite).saveAsTable(schema+".mid_ass_ta_cust_income_detail");
        //hc.refreshTable(schema+".mid_fact_custincomechg_detail");
        //System.out.println("count="+hc.sql("select * from test.mid_fact_custincomechg_detail").count());

        sql = "insert overwrite table " + schema + ".ass_ta_cust_income_detail partition(dk_system_of_upd,effective_from) " +
                "    select    " +
                "         case when b.cserialno is not null and a.cserialno is not null then a.trd_it_trx_serialno         when b.cserialno is not null then b.trd_it_trx_serialno          else  a.trd_it_trx_serialno         end as  trd_it_trx_serialno   ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_tano         			   when b.cserialno is not null then b.dk_tano                      else  a.dk_tano                     end as  dk_tano               ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.cserialno                   when b.cserialno is not null then b.cserialno                    else  a.cserialno                   end as  cserialno             ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ori_cserialno               when b.cserialno is not null then b.ori_cserialno                else  a.ori_cserialno               end as  ori_cserialno         ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_invpty_of_cust           when b.cserialno is not null then b.sk_invpty_of_cust            else  a.sk_invpty_of_cust           end as  sk_invpty_of_cust     ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_cust_type      		   when b.cserialno is not null then b.dk_cust_type                 else  a.dk_cust_type                end as  dk_cust_type          ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_account_of_fd            when b.cserialno is not null then b.sk_account_of_fd             else  a.sk_account_of_fd            end as  sk_account_of_fd      ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_tradeacco_reg            when b.cserialno is not null then b.sk_tradeacco_reg             else  a.sk_tradeacco_reg            end as  sk_tradeacco_reg      ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_currency                 when b.cserialno is not null then b.sk_currency                  else  a.sk_currency                 end as  sk_currency           ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.agencyno                    when b.cserialno is not null then b.agencyno                     else  a.agencyno    		        end as  agencyno              ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.netno                       when b.cserialno is not null then b.netno                        else  a.netno      			        end as  netno            	  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_product                  when b.cserialno is not null then b.sk_product   		        else  a.sk_product     			    end as  sk_product      	  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_share_type               when b.cserialno is not null then b.dk_share_type                else  a.dk_share_type               end as  dk_share_type         ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_agency         		   when b.cserialno is not null then b.sk_agency        		    else  a.sk_agency       		    end as  sk_agency             ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.reg_date                    when b.cserialno is not null then b.reg_date         		    else  a.reg_date       		        end as  reg_date              ," +
                "         case when b.cserialno is not null and a.cserialno is not null then b.effective_to                when b.cserialno is not null then b.effective_to                 else  a.effective_to                end as  effective_to          ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ori_net_value               when b.cserialno is not null then b.ori_net_value                else  a.ori_net_value               end as  ori_net_value         ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ori_sk_mkt_trade_type       when b.cserialno is not null then b.ori_sk_mkt_trade_type        else  a.ori_sk_mkt_trade_type       end as  ori_sk_mkt_trade_type ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sk_mkt_trade_type           when b.cserialno is not null then b.sk_mkt_trade_type            else  a.sk_mkt_trade_type           end as  sk_mkt_trade_type     ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_bourseflag               when b.cserialno is not null then b.dk_bourseflag                else  a.dk_bourseflag               end as  dk_bourseflag         ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ori_shares                  when b.cserialno is not null then b.ori_shares                   else  a.ori_shares                  end as  ori_shares            ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ori_cost                    when b.cserialno is not null then b.ori_cost                     else  a.ori_cost                    end as  ori_cost              ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.last_shares    			   when b.cserialno is not null then b.last_shares                  else  a.last_shares                 end as  last_shares   		  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.share_change    			   when b.cserialno is not null then b.share_change                 else  a.share_change                end as  share_change 		  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.shares                      when b.cserialno is not null then b.shares          		        else  a.shares          		    end as  shares                ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.cost                        when b.cserialno is not null then b.cost                         else  a.cost    				    end as  cost    			  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.income                      when b.cserialno is not null then b.income  		                else  a.income                      end as  income       	      ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.income_incld_cost           when b.cserialno is not null then b.income_incld_cost            else  a.income_incld_cost           end as  income_incld_cost     ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.total_cost                  when b.cserialno is not null then b.total_cost     		        else  a.total_cost   		        end as  total_cost      	  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.total_income                when b.cserialno is not null then b.total_income 			    else  a.total_income 			    end as  total_income		  ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.total_income_incld_cost     when b.cserialno is not null then b.total_income_incld_cost      else  a.total_income_incld_cost     end as  total_income_incld_cos   ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_is_valid                 when b.cserialno is not null then b.dk_is_valid 				    else  a.dk_is_valid 				end as  dk_is_valid				 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_system_of_sdata          when b.cserialno is not null then b.dk_system_of_sdata           else  a.dk_system_of_sdata          end as  dk_system_of_sdata       ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.sdata_serialno              when b.cserialno is not null then b.sdata_serialno               else  a.sdata_serialno              end as  sdata_serialno           ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.memo                        when b.cserialno is not null then b.memo          			    else  a.memo      			        end as  memo         			 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.ddvc                        when b.cserialno is not null then b.ddvc       				    else  a.ddvc       				    end as  ddvc      				 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.batchno                     when b.cserialno is not null then b.batchno 				        else  a.batchno   				    end as  batchno    				 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.inserttime  				   when b.cserialno is not null then b.inserttime   				else  a.inserttime 				    end as  inserttime 				 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then b.updatetime                  when b.cserialno is not null then b.updatetime           	    else  a.updatetime                  end as  updatetime               ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.dk_system_of_upd            when b.cserialno is not null then b.dk_system_of_upd   		    else  a.dk_system_of_upd            end as  dk_system_of_upd		 ," +
                "         case when b.cserialno is not null and a.cserialno is not null then a.effective_from              when b.cserialno is not null then b.effective_from   		    else  a.effective_from              end as  effective_from " +
                "    from " + schema + ".ass_ta_cust_income_detail a" +
                "    full outer join " + schema + ".mid_ass_ta_cust_income_detail b" +
                "      on  a.ori_cserialno = b.ori_cserialno" +
                "     and a.cserialno = b.cserialno" +
                "     and a.dk_system_of_upd = b.dk_system_of_upd ";
        System.out.println("================start====sql====3==============");
        System.out.println(sql);
        System.out.println("================end====sql======3============");
        hc.sql(sql);
        d_sp_startdate = add(d_sp_enddate, step);
        //} //end while

        //Thread.currentThread().sleep(1000*1000L);
        hc.close();

    }


    static JavaRDD<Iterable<Row>> handleNext2(Dataset<Row> adf, final Dataset<Row> lookdf, StructType st) {

        JavaPairRDD<Object, Row> ardd = adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();
        JavaPairRDD<Object, Row> lkprdd = lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();

        JavaPairRDD<Object, Tuple2<Iterable<Row>, Iterable<Row>>> result = ardd.cogroup(lkprdd);


        JavaRDD<Iterable<Row>> w = result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>, Iterable<Row>>() {

            public static final double err = 0.000001d;
            private static final long serialVersionUID = 7194267611504718718L;

            @Override
            public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception {
                List<Row> ls = new ArrayList<Row>();
                Iterable<Row> tr = tuple._2._1;
                Iterable<Row> vr = tuple._2._2;

                java.util.Iterator<Row> itr = tr.iterator();
                java.util.Iterator<Row> ivr = vr.iterator();

                Row tranRow = null;
                Row currRow = null;
                double rmshares = 0;//剩余要扣除份额

                boolean findNext = true;//同一天两笔去扣件一笔
                while (itr.hasNext()) {
                    tranRow = itr.next();
                    rmshares = ((BigDecimal) tranRow.getAs("confirm_shares".toUpperCase())).doubleValue();//从确认份额取出代扣份额
                    double[] d = new double[]{rmshares, 0};
                    while (rmshares > err)//份额没有扣完
                    {
                        if (findNext) //前面的记录已经扣完，需要取下一条记录。
                        {
                            if (ivr.hasNext()) {
                                currRow = ivr.next();
                                //基于赎回记录，修改当前记录的effective_to，并返回
                                ls.add(changeEt(currRow, tranRow, st));
                            } else {   //不够扣减，则应该报错。
                                break;//跳出
                            }
                        }

                        if (!findNext) {
                            //基于上次留下记录继续处理
                            //中间状态的先入库
                            ls.add(changeEt3(currRow, tranRow, st));
                        }

                        //实施扣减,扣减完effective_to=99991231
                        Row ret = changeEt2(currRow, tranRow, d, st);
                        if (((BigDecimal) ret.getAs(st.fieldIndex("shares".toUpperCase()))).doubleValue() < err) {
                            //剩余份额为0，不会再参加处理，直接入库
                            findNext = true;
                            ls.add(ret);
                            currRow = null;
                        } else {
                            //返回记录留作下一次扣减
                            currRow = ret;
                            findNext = false;
                        }
                        rmshares = d[0];//返回剩余代扣减份额
                    }//份额扣减完成
                }//份额减少记录处理完成
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


    //扣减过程，返回剩余代扣减份额
    static Row changeEt2(Row currRow, Row tranRow, double[] d, StructType st) {
        double rmshares = d[0];
        double tmptotalincome = d[1];

        double orinetvalue = ((BigDecimal) currRow.getAs(st.fieldIndex("ori_net_value".toUpperCase()))).doubleValue();
        double netvalue = ((BigDecimal) tranRow.getAs("net_value".toUpperCase())).doubleValue();
        double tr_shares = ((BigDecimal) tranRow.getAs("confirm_shares".toUpperCase())).doubleValue();
        double tr_amount = ((BigDecimal) tranRow.getAs("confirm_balance".toUpperCase())).doubleValue();
        double totalincome = ((BigDecimal) currRow.getAs(st.fieldIndex("total_income".toUpperCase()))).doubleValue();
        double oricost = ((BigDecimal) currRow.getAs(st.fieldIndex("ori_cost".toUpperCase()))).doubleValue();
        double orishares = ((BigDecimal) currRow.getAs(st.fieldIndex("ori_shares".toUpperCase()))).doubleValue();
        //带走的总成本
        double totalcostofincome = ((BigDecimal) currRow.getAs(st.fieldIndex("total_income_incld_cost".toUpperCase()))).doubleValue();
        int incomerule = ((BigDecimal) tranRow.getAs("income_rule".toUpperCase())).intValue();

        double old_shares = ((BigDecimal) currRow.getAs(st.fieldIndex("shares".toUpperCase()))).doubleValue();//保有份额
        double new_shares = 0;

        if (old_shares - rmshares >= 0) //保有份额大于等于要扣减份额
            new_shares = old_shares - rmshares;
        else
            new_shares = 0;

        double shrchg = 0;//如果保有份额小于等于要扣减份额，则变化份额只能是保有份额
        if (old_shares <= rmshares)
            shrchg = old_shares;
        else
            shrchg = rmshares;

        rmshares += (-1.0 * shrchg);

        double costofincome = 0;//收益对应成本
        double income = 0;

        if (incomerule == 1)//交易确认金额计算收益
        {
            if (oricost - totalcostofincome > 0) {
                costofincome = Double.parseDouble(new DecimalFormat("#.00").format(shrchg / orishares * oricost));
            }

            if (rmshares > 0) {
                income = shrchg / tr_shares * tr_amount;
                tmptotalincome += income;//多笔扣减，保证最后一笔钆差
            } else {
                income = tr_amount - tmptotalincome;
            }
        } else if (incomerule == 2)//根据净值计算确认金额，强减？
        {
            costofincome = shrchg * orinetvalue;
            income = shrchg * netvalue;
        } else if (incomerule == 0)//不产生收益
        {
            costofincome = 0;
            income = 0;
        }

        totalincome += income;
        totalcostofincome += costofincome;

        d[0] = rmshares;
        d[1] = tmptotalincome;

        return RowFactory.create(
                currRow.getAs(st.fieldIndex("hashcode".toUpperCase())),//1
                null,//2
                currRow.getAs(st.fieldIndex("dk_tano".toUpperCase())),//3
                tranRow.getAs("cserialno".toUpperCase()), //4
                currRow.getAs(st.fieldIndex("ori_cserialno".toUpperCase())), //5
                currRow.getAs(st.fieldIndex("sk_invpty_of_cust".toUpperCase())), //6
                currRow.getAs(st.fieldIndex("dk_cust_type".toUpperCase())), //7
                currRow.getAs(st.fieldIndex("sk_account_of_fd".toUpperCase())),// 8
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg".toUpperCase())), //9
                currRow.getAs(st.fieldIndex("sk_currency".toUpperCase())), //10
                currRow.getAs(st.fieldIndex("agencyno".toUpperCase())), //11
                currRow.getAs(st.fieldIndex("netno".toUpperCase())), //12
                currRow.getAs(st.fieldIndex("sk_product".toUpperCase())),//13
                currRow.getAs(st.fieldIndex("dk_share_type".toUpperCase())), //14
                currRow.getAs(st.fieldIndex("sk_agency".toUpperCase())), //15
                currRow.getAs(st.fieldIndex("reg_date".toUpperCase())), //16
                new BigDecimal("99991231"), //effective_to 17
                currRow.getAs(st.fieldIndex("ori_net_value".toUpperCase())), //18
                currRow.getAs(st.fieldIndex("ori_sk_mkt_trade_type".toUpperCase())), //19
                tranRow.getAs("sk_mkt_trade_type".toUpperCase()), //20
                tranRow.getAs("dk_bourseflag".toUpperCase()), //21
                currRow.getAs(st.fieldIndex("ori_shares".toUpperCase())), //22
                currRow.getAs(st.fieldIndex("ori_cost".toUpperCase())), //23
                currRow.getAs(st.fieldIndex("shares".toUpperCase())), //last_shares 24
                new BigDecimal(-1.0 * shrchg), //share_change 25
                new BigDecimal(new_shares), //shares 26 剩余份额
                new BigDecimal(0), //cost 27
                new BigDecimal(income), //income 28
                new BigDecimal(costofincome), //income_incld_cost 29
                currRow.getAs(st.fieldIndex("total_cost".toUpperCase())), //total_cost 30
                new BigDecimal(totalincome), //total_income 31
                new BigDecimal(totalcostofincome), //total_income_incld_cost 32
                tranRow.getAs("back_fee".toUpperCase()), //33
                null,//dk_is_valid 34
                null,//dk_system_of_sdata 35
                null,//sdata_serialno 36
                "new",//memo 37
                null,//ddvc 38
                tranRow.getAs("batchno".toUpperCase()), // 39
                new java.sql.Timestamp(System.currentTimeMillis()), //40
                new java.sql.Timestamp(System.currentTimeMillis()), //41
                tranRow.getAs("dk_system_of_upd".toUpperCase()),//42
                tranRow.getAs("ta_cfm_date".toUpperCase()) //effective_from 43
        );
    }


    //只是截断effective_to和更新updatetime，老的那条已经在表里面，
    static Row changeEt(Row currRow, Row tranRow, StructType st) {
        return RowFactory.create(
                currRow.getAs(st.fieldIndex("hashcode".toUpperCase())),// 1
                currRow.getAs(st.fieldIndex("trd_it_trx_serialno".toUpperCase())),//2
                null, //3
                null, //4
                null, //5
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                tranRow.getAs("ta_cfm_date".toUpperCase()), //effective_to //17
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new java.sql.Timestamp(System.currentTimeMillis()), //41
                null,
                null
        );
    }

    //每笔扣减的中间状态都需要进入表
    static Row changeEt3(Row currRow, Row tranRow, StructType st) {
        return RowFactory.create(
                currRow.getAs(st.fieldIndex("hashcode".toUpperCase())),//1
                currRow.getAs(st.fieldIndex("trd_it_trx_serialno".toUpperCase())),//2
                currRow.getAs(st.fieldIndex("dk_tano".toUpperCase())),//3
                currRow.getAs(st.fieldIndex("cserialno".toUpperCase())), //4
                currRow.getAs(st.fieldIndex("ori_cserialno".toUpperCase())), //5
                currRow.getAs(st.fieldIndex("sk_invpty_of_cust".toUpperCase())), //6
                currRow.getAs(st.fieldIndex("dk_cust_type".toUpperCase())), //7
                currRow.getAs(st.fieldIndex("sk_account_of_fd".toUpperCase())),// 8
                currRow.getAs(st.fieldIndex("sk_tradeacco_reg".toUpperCase())), //9
                currRow.getAs(st.fieldIndex("sk_currency".toUpperCase())), //10
                currRow.getAs(st.fieldIndex("agencyno".toUpperCase())), //11
                currRow.getAs(st.fieldIndex("netno".toUpperCase())), //12
                currRow.getAs(st.fieldIndex("sk_product".toUpperCase())),//13
                currRow.getAs(st.fieldIndex("dk_share_type".toUpperCase())), //14
                currRow.getAs(st.fieldIndex("sk_agency".toUpperCase())), //15
                currRow.getAs(st.fieldIndex("reg_date".toUpperCase())), //16
                tranRow.getAs("ta_cfm_date".toUpperCase()), //effective_to 17
                currRow.getAs(st.fieldIndex("ori_net_value".toUpperCase())), //18
                currRow.getAs(st.fieldIndex("ori_sk_mkt_trade_type".toUpperCase())), //19
                currRow.getAs(st.fieldIndex("sk_mkt_trade_type".toUpperCase())), //20
                currRow.getAs(st.fieldIndex("dk_bourseflag".toUpperCase())), //21
                currRow.getAs(st.fieldIndex("ori_shares".toUpperCase())), //22
                currRow.getAs(st.fieldIndex("ori_cost".toUpperCase())), //23
                currRow.getAs(st.fieldIndex("last_shares".toUpperCase())), //last_shares 24
                currRow.getAs(st.fieldIndex("share_change".toUpperCase())), //share_change 25
                currRow.getAs(st.fieldIndex("shares".toUpperCase())), //shares 26
                currRow.getAs(st.fieldIndex("cost".toUpperCase())),//cost 27
                currRow.getAs(st.fieldIndex("income".toUpperCase())), //income 28
                currRow.getAs(st.fieldIndex("income_incld_cost".toUpperCase())), //income_incld_cost 29
                currRow.getAs(st.fieldIndex("total_cost".toUpperCase())), //total_cost 30
                currRow.getAs(st.fieldIndex("total_income".toUpperCase())),//total_income 31
                currRow.getAs(st.fieldIndex("total_income_incld_cost".toUpperCase())),//total_income_incld_cost 32
                currRow.getAs(st.fieldIndex("back_fee".toUpperCase())), //33
                currRow.getAs(st.fieldIndex("dk_is_valid".toUpperCase())),//dk_is_valid 34
                currRow.getAs(st.fieldIndex("dk_system_of_sdata".toUpperCase())),//dk_system_of_sdata 35
                currRow.getAs(st.fieldIndex("sdata_serialno".toUpperCase())),//sdata_serialno 36
                currRow.getAs(st.fieldIndex("memo".toUpperCase())),//memo 37
                currRow.getAs(st.fieldIndex("ddvc".toUpperCase())),//ddvc 38
                currRow.getAs(st.fieldIndex("batchno".toUpperCase())), //39
                currRow.getAs(st.fieldIndex("inserttime".toUpperCase())), //40
                currRow.getAs(st.fieldIndex("updatetime".toUpperCase())),//41
                currRow.getAs(st.fieldIndex("dk_system_of_upd".toUpperCase())),   //42
                currRow.getAs(st.fieldIndex("effective_from".toUpperCase())) //43
        );
    }

    static Date add(Date d, int days) {
        return new Date(d.getTime() + days * 24 * 60 * 60 * 1000L);
    }
}
