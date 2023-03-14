package cn.com.businessmatrix;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class SparkPL2 {

	public static long val = 0;
	public static List<Long[]> list = null;
	public static Map<Long, List<Long>> map = null;
    public static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	public static void main(String[] args) throws Exception{
			
		String srcsys=args[0];
		String batchno=args[1];
		String startdate=args[2];
		String enddate=args[3];	
		String schema=args[4];
		int step=Integer.parseInt(args[5]);
		 
		Date d_startdate=sdf.parse(startdate);
		Date d_enddate=sdf.parse(enddate);
		Date d_sp_startdate=d_startdate;
		Date d_sp_enddate=null;
		
		Map<String,String> dBConOption = new HashMap<String,String>();
		dBConOption.put("url", "jdbc:impala://10.64.36.70:25003/ods;AuthMech=3");
		dBConOption.put("user", "etl_az");
		dBConOption.put("password", "123456");
		dBConOption.put("driver", "com.cloudera.impala.jdbc41.Driver");
		 
	
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
		            .appName("SparkPL2")
		            //.master("local[4]")
		            //.master("yarn-cluster")
		            .getOrCreate();
		 DataFrameReader dfRead= hc.read().format("jdbc").options(dBConOption);
   		
	
		while(true)
		{
			  
			String sql_lookdf=
					"(select cast(fnv_hash("
					+ "concat("
					+ "cast(t1.sk_account_of_fd as string),"
					+ "cast(t1.sk_tradeacco_reg as string),"
					+ "cast(t1.sk_product       as string),"
					+ "t1.dk_share_type,"
					+ "t1.agencyno,"
					+ "t1.netno)) as string)  as hashcode, " +
										"       t1.trd_it_trx_serialno,"+
										"       t1.dk_tano,"+
										"       t1.cserialno, " + 
										"       t1.ori_cserialno, " + 
										"       t1.sk_invpty_of_cust, " + 
										"       t1.dk_cust_type, " + 
										"       t1.sk_account_of_fd, " + 
										"       t1.sk_tradeacco_reg, " + 
										"       t1.sk_currency, " +  
										"       t1.agencyno, " + 
										"       t1.netno, " + 
										"       t1.sk_product, " + 
										"       t1.dk_share_type, " + 
										"       t1.sk_agency, " + 
										"       t1.reg_date, " + 
										"       t2.effective_to, " + 
										"       t1.ori_net_value, " + 
										"       t1.ori_sk_mkt_trade_type, " + 
										"       t1.sk_mkt_trade_type, " + 
										"       t1.dk_bourseflag, " + 
										"       t1.ori_shares, " + 
										"       t1.ori_cost, " + 
										"       t1.last_shares, " + 
										"       t1.share_change, " + 
										"       t1.shares, " + 
										"       t1.cost, " + 
										"       t1.income, " + 
										"       t1.income_incld_cost, " + 
										"       t1.total_cost, " + 
										"       t1.total_income, " + 
										"       t1.total_income_incld_cost, " + 
										"       t1.back_fee, " + 
										"       t1.dk_is_valid, " + 
										"       t1.dk_system_of_sdata, " + 
										"       t1.sdata_serialno, " + 
										"       t1.memo, " + 
										"       t1.ddvc, " + 
										"       t1.batchno, " + 
										"       t1.inserttime, " + 
										"       t1.updatetime, " + 
										"       t1.dk_system_of_upd, " + 
										"       t1.effective_from " + 
										"  from "+schema+".ass_ta_cust_income_detail t1 inner join "+
										schema+".ass_ta_cust_income_detail_et t2 on t1.trd_it_trx_serialno=t2.trd_it_trx_serialno"+ 
										"  where t1.dk_system_of_upd='"+srcsys+"' and t1.shares>0 and t2.effective_to=99991231 "+
										" and t1.sk_account_of_fd in(9370,13270)) as tt"
										;
			System.out.println(sql_lookdf);			
			Dataset<Row> lookdf=dfRead.option("dbtable", "vw_ass_ta_cust_income_detail").load();
			lookdf=lookdf//.filter(lookdf.col("dk_system_of_upd").equalTo(srcsys))
					.repartition(lookdf.col("hashcode"))
					.sort(lookdf.col("reg_date").asc(), lookdf.col("ori_cserialno").asc());
			
			StructType st=lookdf.schema();
			System.out.println();		
		   lookdf.printSchema();
		   
			if(d_sp_startdate.after(d_enddate))
				break;
			d_sp_enddate=add(d_sp_startdate,step);
			if(d_sp_enddate.after(d_enddate))	
				d_sp_enddate=d_enddate;
			
			String sql=	
					"(select cast(fnv_hash("
							+ "concat("
							+ "cast(t.sk_account_of_fd as string),"
							+ "cast(t.sk_tradeacco_reg as string),"
							+ "cast(t.sk_product       as string),"
							+ "t.dk_share_type,"
							+ "t.agencyno,"
							+ "t.netno)) as string) as hashcode " + 
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
									"               ,t.confirm_balance " + 
									"               ,t.confirm_shares " + 
									"               ,t.back_fee " + 
									"               ,t.net_value " + 
									"               ,t.requestno " + 
									"               ,t.dk_bonus_type " +  
									"               ,t.dk_bourseflag" + 
									"               ,t.dk_system_of_upd " + 
									"               ,t.ta_cfm_date " +  
									"               ,m.shr_chg_dir " +  
									"               ,m.income_rule " + 
									"               ,t.batchno " + 
									"  from "+schema+".trd_ta_saletran t "+
									"  left join  "+schema+".comm_mkt_tradetype m on t.sk_mkt_trade_type=m.sk_mkt_trade_type "+
									//" inner join ods.prod_nav p ON t.sk_product=p.sk_product and t.ta_cfm_date=p.ta_cfm_date "+
									" left join  "+schema+".prod_asset_product pp on t.sk_product=pp.sk_product "+							  
								    " where t.ta_cfm_date between "+sdf.format(d_sp_startdate) +" and "+sdf.format(d_sp_enddate) +
								    " and t.dk_system_of_upd='"+srcsys+"' and m.shr_chg_dir = -1 "+
								    " and pp.dk_money_fund='N' and t.dk_saletran_status='1' "+
								    " and t.sk_account_of_fd=13270 ) as tt"
								    ;
			System.out.println();			
			System.out.println(sql);	
			
			Dataset<Row> adf =  dfRead.option("dbtable", "vw_trd_ta_saletran").load();
			adf=adf//.filter(adf.col("ta_cfm_date").between(sdf.format(d_sp_startdate), sdf.format(d_sp_enddate)))
					.repartition(adf.col("hashcode"))
					.sort(adf.col("ta_cfm_date").asc(), adf.col("cserialno").asc());
			System.out.println();		
			adf.printSchema();
			System.out.println();
			JavaRDD<Iterable<Row>> w=handleNext2(adf,lookdf,st);
		    JavaRDD<Row> x=w.flatMap(new FlatMapFunction<Iterable<Row>,Row>(){
				private static final long serialVersionUID = -6111647437563220114L;

				@Override
				public java.util.Iterator<Row> call(Iterable<Row> arg0) throws Exception {
						return arg0.iterator();
				}
		    	
		    });
		    
		    //System.out.println(x.count());
		    //hc.sql("truncate table "+schema+".mid_fact_custincomechg_detail");	    
		   // hc.createDataFrame(x, lookdf.schema()).registerTempTable("c");
		    //hc.sql("insert into table  "+schema+".mid_ass_ta_cust_income_detail  select * from c");
		    hc.createDataFrame(x, lookdf.schema()).write().format("parquet").mode(SaveMode.Overwrite)
		    .saveAsTable(schema+".mid_ass_ta_cust_income_detail");
		    //hc.refreshTable(schema+".mid_fact_custincomechg_detail");
		    //System.out.println("count="+hc.sql("select * from test.mid_fact_custincomechg_detail").count());
		   
			d_sp_startdate=add(d_sp_enddate,step);			
		} //end while
		
		//Thread.currentThread().sleep(1000*1000L);
		hc.close();
		
	}

	
   static JavaRDD<Iterable<Row>> handleNext2(Dataset<Row> adf,final Dataset<Row> lookdf,StructType st) {
	   
	   JavaPairRDD<Object, Row> ardd =adf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();
	   JavaPairRDD<Object, Row> lkprdd =lookdf.toJavaRDD().mapPartitionsToPair(new JavaPairFlatMapPartitionFunc()).cache();
	    
	   JavaPairRDD<Object,Tuple2<Iterable<Row>,Iterable<Row>>> result=ardd.cogroup(lkprdd);
	   
	  
	   JavaRDD<Iterable<Row>> w=result.map(new Function<Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>>,Iterable<Row>>() {
           
		public static final  double err=0.000001d;
		private static final long serialVersionUID = 7194267611504718718L;

		@Override
           public Iterable<Row> call(Tuple2<Object, Tuple2<Iterable<Row>, Iterable<Row>>> tuple) throws Exception { 
			   List<Row> ls=new ArrayList<Row>();
               Iterable<Row> tr=tuple._2._1;
               Iterable<Row> vr=tuple._2._2;
               
               java.util.Iterator<Row> itr=tr.iterator();
               java.util.Iterator<Row> ivr=vr.iterator();
               
               Row tranRow=null;
               Row currRow=null;
               double rmshares=0;//剩余要扣除份额
               
               boolean findNext=true;//同一天两笔去扣件一笔
               while(itr.hasNext())
               {
            	   tranRow=itr.next();          	  
            	   rmshares=((BigDecimal)tranRow.getAs("confirm_shares")).doubleValue();//从确认份额取出代扣份额
            	   double[] d=new double[]{rmshares,0};
            	   while(rmshares>err)//份额没有扣完
            	   { 
            		     if(findNext) //前面的记录已经扣完，需要取下一条记录。
            		     {
            		    	 if(ivr.hasNext())
            		    	 {
            		    		 currRow=ivr.next();
            		    		 //基于赎回记录，修改当前记录的effective_to，并返回
            		    		 ls.add(changeEt(currRow,tranRow,st));
            		    	 }
            		    	 else
            		    	 {   //不够扣减，则应该报错。
            		    		 break;//跳出
            		    	 }
            		     }
            		     
            		     if(!findNext)
            		     {
            		    	 //基于上次留下记录继续处理
            		    	 //中间状态的先入库
            		    	 ls.add(changeEt3(currRow,tranRow,st));    
            		     }
            		     
            		    //实施扣减,扣减完effective_to=99991231
                	     Row ret=changeEt2(currRow,tranRow,d,st);              			      			  
                		 if(((BigDecimal)ret.getAs(st.fieldIndex("shares"))).doubleValue()<err)
               			  {
               				  //剩余份额为0，不会再参加处理，直接入库
               				 findNext=true;
                			 ls.add(ret);
                   		 	 currRow=null;
                		  }
                		  else
               			  {
               				  //返回记录留作下一次扣减
               				  currRow=ret;
               				  findNext=false;
               			  } 		     
            		  rmshares=d[0];//返回剩余代扣减份额
            	   }//份额扣减完成
               }//份额减少记录处理完成
               return ls;
           }
       });
	   
	   return w.filter(new Function<Iterable<Row>,Boolean>(){
		private static final long serialVersionUID = -430944040308979866L;

		@Override
		public Boolean call(Iterable<Row> arg0) throws Exception {
			if(arg0.iterator().hasNext())
				return true;
			else
				return false;
		}
		   
	   });
	}
    
   
   //扣减过程，返回剩余代扣减份额
   static Row  changeEt2(Row currRow,Row tranRow,double[] d,StructType st)
   {
	double rmshares=d[0];
	double tmptotalincome=d[1];
	
	double orinetvalue=((BigDecimal)currRow.getAs(st.fieldIndex("ori_net_value"))).doubleValue();
	double netvalue=((BigDecimal)tranRow.getAs("net_value")).doubleValue();
	double tr_shares=((BigDecimal)tranRow.getAs("confirm_shares")).doubleValue();
	double tr_amount=((BigDecimal)tranRow.getAs("confirm_balance")).doubleValue();
	double totalincome=((BigDecimal)currRow.getAs(st.fieldIndex("total_income"))).doubleValue();
	double oricost=((BigDecimal)currRow.getAs(st.fieldIndex("ori_cost"))).doubleValue();
	double orishares=((BigDecimal)currRow.getAs(st.fieldIndex("ori_shares"))).doubleValue();
	//带走的总成本
	double totalcostofincome=((BigDecimal)currRow.getAs(st.fieldIndex("total_income_incld_cost"))).doubleValue();
	int incomerule=((BigDecimal)tranRow.getAs("income_rule")).intValue();
	
	double old_shares=((BigDecimal)currRow.getAs(st.fieldIndex("shares"))).doubleValue();//保有份额
	double new_shares=0;
	
	if(old_shares-rmshares>=0) //保有份额大于等于要扣减份额
		new_shares=old_shares-rmshares;
	else 
		new_shares=0;
	
	double shrchg=0;//如果保有份额小于等于要扣减份额，则变化份额只能是保有份额
	if(old_shares<=rmshares)
		shrchg=old_shares;
	else
		shrchg=rmshares;
	
	rmshares+=(-1.0*shrchg);
	
	double costofincome=0;//收益对应成本
	double income=0;
	
	if(incomerule==1)//交易确认金额计算收益
	{
		if(oricost-totalcostofincome>0)
		{
			costofincome=Double.parseDouble(new DecimalFormat("#.00").format(shrchg/orishares*oricost));
		}
		
		if(rmshares>0)
		{
			income=shrchg/tr_shares*tr_amount;
			tmptotalincome+=income;//多笔扣减，保证最后一笔钆差
		}
		else
		{
			income=tr_amount-tmptotalincome;
		}		 
	}
	else if(incomerule==2)//根据净值计算确认金额，强减？
	{		 
		costofincome=shrchg*orinetvalue;	
		income	    =shrchg*netvalue;
	}
	else if(incomerule==0)//不产生收益
	{
		costofincome=0;
		income=0; 
	}
	
	totalincome+=income;
	totalcostofincome+=costofincome;
	
	d[0]=rmshares;
	d[1]=tmptotalincome; 
		
   	return RowFactory.create(
   			        currRow.getAs(st.fieldIndex("hashcode")),//1
   			        null,//2
   			        currRow.getAs(st.fieldIndex("dk_tano")),//3
   			        tranRow.getAs("cserialno"), //4
   					currRow.getAs(st.fieldIndex("ori_cserialno")), //5
   					currRow.getAs(st.fieldIndex("sk_invpty_of_cust")), //6
   					currRow.getAs(st.fieldIndex("dk_cust_type")), //7
   					currRow.getAs(st.fieldIndex("sk_account_of_fd")),// 8
   					currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //9
   					currRow.getAs(st.fieldIndex("sk_currency")), //10
   					currRow.getAs(st.fieldIndex("agencyno")), //11
   					currRow.getAs(st.fieldIndex("netno")), //12
   					currRow.getAs(st.fieldIndex("sk_product")),//13 
   					currRow.getAs(st.fieldIndex("dk_share_type")), //14
   					currRow.getAs(st.fieldIndex("sk_agency")), //15
   					currRow.getAs(st.fieldIndex("reg_date")), //16
   					new BigDecimal("99991231"), //effective_to 17
   					currRow.getAs(st.fieldIndex("ori_net_value")), //18
   					currRow.getAs(st.fieldIndex("ori_sk_mkt_trade_type")), //19
   					tranRow.getAs("sk_mkt_trade_type"), //20
   					tranRow.getAs("dk_bourseflag"), //21
   					currRow.getAs(st.fieldIndex("ori_shares")), //22
   					currRow.getAs(st.fieldIndex("ori_cost")), //23
   					currRow.getAs(st.fieldIndex("shares")), //last_shares 24
   					new BigDecimal(-1.0*shrchg), //share_change 25
   					new BigDecimal(new_shares), //shares 26 剩余份额
   					new BigDecimal(0), //cost 27
   					new BigDecimal(income), //income 28
   					new BigDecimal(costofincome), //income_incld_cost 29 
   					currRow.getAs(st.fieldIndex("total_cost")), //total_cost 30
   					new BigDecimal(totalincome), //total_income 31
   					new BigDecimal(totalcostofincome), //total_income_incld_cost 32
   					tranRow.getAs("back_fee"), //33
   					null,//dk_is_valid 34
   					null,//dk_system_of_sdata 35
   					null,//sdata_serialno 36
   					"new",//memo 37
   					null,//ddvc 38
   					tranRow.getAs("batchno"), // 39
   					new java.sql.Timestamp(System.currentTimeMillis()), //40
   					new java.sql.Timestamp(System.currentTimeMillis()), //41
   					tranRow.getAs("dk_system_of_upd") ,//42
   					tranRow.getAs("ta_cfm_date") //effective_from 43
   			);
   }
    
   
   //只是截断effective_to和更新updatetime，老的那条已经在表里面，
   static Row  changeEt(Row currRow,Row tranRow,StructType st)
    {
    	return RowFactory.create(
    			    currRow.getAs(st.fieldIndex("hashcode")),// 1 
    			    currRow.getAs(st.fieldIndex("trd_it_trx_serialno")),//2
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
					tranRow.getAs("ta_cfm_date"), //effective_to //17
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
   static Row  changeEt3(Row currRow,Row tranRow,StructType st)
   {   
	   return RowFactory.create(
			        currRow.getAs(st.fieldIndex("hashcode")),//1
			        currRow.getAs(st.fieldIndex("trd_it_trx_serialno")),//2
			        currRow.getAs(st.fieldIndex("dk_tano")),//3
			        currRow.getAs(st.fieldIndex("cserialno")), //4
					currRow.getAs(st.fieldIndex("ori_cserialno")), //5
					currRow.getAs(st.fieldIndex("sk_invpty_of_cust")), //6
					currRow.getAs(st.fieldIndex("dk_cust_type")), //7
					currRow.getAs(st.fieldIndex("sk_account_of_fd")),// 8
					currRow.getAs(st.fieldIndex("sk_tradeacco_reg")), //9
					currRow.getAs(st.fieldIndex("sk_currency")), //10
					currRow.getAs(st.fieldIndex("agencyno")), //11
					currRow.getAs(st.fieldIndex("netno")), //12
					currRow.getAs(st.fieldIndex("sk_product")),//13 
					currRow.getAs(st.fieldIndex("dk_share_type")), //14
					currRow.getAs(st.fieldIndex("sk_agency")), //15
					currRow.getAs(st.fieldIndex("reg_date")), //16
					tranRow.getAs("ta_cfm_date"), //effective_to 17
					currRow.getAs(st.fieldIndex("ori_net_value")), //18
					currRow.getAs(st.fieldIndex("ori_sk_mkt_trade_type")), //19
					currRow.getAs(st.fieldIndex("sk_mkt_trade_type")), //20
					currRow.getAs(st.fieldIndex("dk_bourseflag")), //21
					currRow.getAs(st.fieldIndex("ori_shares")), //22
					currRow.getAs(st.fieldIndex("ori_cost")), //23
					currRow.getAs(st.fieldIndex("last_shares")), //last_shares 24
					currRow.getAs(st.fieldIndex("share_change")), //share_change 25
					currRow.getAs(st.fieldIndex("shares")), //shares 26
					currRow.getAs(st.fieldIndex("cost")),//cost 27
					currRow.getAs(st.fieldIndex("income")), //income 28
					currRow.getAs(st.fieldIndex("income_incld_cost")), //income_incld_cost 29
					currRow.getAs(st.fieldIndex("total_cost")), //total_cost 30
					currRow.getAs(st.fieldIndex("total_income")),//total_income 31
					currRow.getAs(st.fieldIndex("total_income_incld_cost")),//total_income_incld_cost 32
					currRow.getAs(st.fieldIndex("back_fee")), //33
					currRow.getAs(st.fieldIndex("dk_is_valid")),//dk_is_valid 34
					currRow.getAs(st.fieldIndex("dk_system_of_sdata")),//dk_system_of_sdata 35
					currRow.getAs(st.fieldIndex("sdata_serialno")),//sdata_serialno 36
					currRow.getAs(st.fieldIndex("memo")),//memo 37
					currRow.getAs(st.fieldIndex("ddvc")),//ddvc 38
					currRow.getAs(st.fieldIndex("batchno")), //39
					currRow.getAs(st.fieldIndex("inserttime")), //40
					currRow.getAs(st.fieldIndex("updatetime")),//41
					currRow.getAs(st.fieldIndex("dk_system_of_upd")),   //42
					currRow.getAs(st.fieldIndex("effective_from")) //43
			);
   }
   
	static Date add(Date d,int days)
	{
		return new Date(d.getTime()+days*24*60*60*1000L);
	}
}
