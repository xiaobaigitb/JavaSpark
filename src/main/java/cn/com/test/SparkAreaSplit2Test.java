package cn.com.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.com.businessmatrix.domain.DateRange;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import cn.com.businessmatrix.utils.SparkSplitUtils;

public class SparkAreaSplit2Test {

	/*
	 * 管理区划拆分
	 */
 
	public static void main(String[] args) {

		 
		SparkConf conf = new SparkConf(); 
		 SparkSession hc = SparkSession.builder()
		            .config(conf) 
		            .appName("ddd")
		            .master("local[1]")
		            //.master("yarn-cluster")
		            .getOrCreate();
		 
		
		Dataset<Row> ipdf =  hc.read().parquet("/Users/Jack/Documents/workspace/spark2/src/org/hesc/example/parquet/ass_ta_fundbal_chg");
		
		
		Dataset<Row> ddf =hc.read().parquet("/Users/Jack/Documents/workspace/spark2/src/org/hesc/example/parquet/org_areasplit_common");

		//Row[] rules = rdf.collect();
		Row[] rs = (Row[]) ddf.orderBy("priority").collect();
		Map<String, List<Row>> ruleMap = new HashMap<String, List<Row>>();
		List<Row> ls = null;
		String key = null;
		for (int i = 0; i < rs.length; i++) {
			key = rs[i].getString(0);
			ls = ruleMap.get(key);
			if (ls == null)
				ls = new ArrayList<Row>();
			ls.add(rs[i]);
			ruleMap.put(key, ls);
		}
		
		
		JavaRDD<List<Row>> w=ipdf.toJavaRDD().map(new Function<Row, List<Row>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public List<Row> call(Row r) throws Exception {
				
				  int  v_cnt0,v_cnt1,v_cnt2,v_cnt3,v_cnt4,v_cnt5,v_cnt6;
				  int  v_count0,v_count1, v_count2, v_count3,v_count4, v_count5,v_count6;
				  
				  String vc_key=null;
				  List<Row> ret=new ArrayList<Row>();
				  
				  Map<String, List<Row>> innerMap = new HashMap<String, List<Row>>();
				 
				 String pi_cserialno =null;//(String)r.getAs("cserialno");
				 String pi_tano =null;//(String)r.getAs("dk_tano");
				 String pi_dk_agency_type =(String)r.getAs("dk_agency_type");
				 String pi_dk_cust_type =(String)r.getAs("dk_cust_type");
				 String pi_agencyno =(String)r.getAs("agencyno");
				 String pi_netno =(String)r.getAs("netno");
				 String pi_dk_share_type =(String)r.getAs("dk_share_type");
				 //String pi_reload_flag =(String)r.getAs("reload_flag");
				 String pi_dk_system_of_sdata=(String)r.getAs("dk_system_of_sdata");
				 String vc_pre_rule_id=null;
				 BigDecimal vc_pre_priority=null;
				 String  vc_pre_dync_flag=null;
 
				 BigDecimal  pi_sk_prod_type       =(BigDecimal)r.getAs("sk_product_type");
				 BigDecimal  pi_sk_product         =(BigDecimal)r.getAs("sk_product");
				 BigDecimal  pi_sk_invpty_type     = null;//(BigDecimal)r.getAs("sk_invpty_type");
				 BigDecimal  pi_sk_invpty          =(BigDecimal)r.getAs("sk_invpty");				  
				 BigDecimal  pi_sk_account_type    = null;//(BigDecimal)r.getAs("sk_account_type");
				 BigDecimal  pi_sk_account         =(BigDecimal)r.getAs("sk_account");
				 BigDecimal  pi_sk_tradeacco_reg   =(BigDecimal)r.getAs("sk_tradeacco_reg");
				 BigDecimal  pi_effective_from     =(BigDecimal)r.getAs("effective_from");
				 BigDecimal  pi_effective_to       =(BigDecimal)r.getAs("effective_to");
				  //pi_trd_serialno     IN NUMBER
				 
				 BigDecimal  pi_lval_ag   =(BigDecimal)r.getAs("lval_ag");
				 BigDecimal  pi_rval_ag   =(BigDecimal)r.getAs("rval_ag");
				 BigDecimal  pi_lval_rg   =(BigDecimal)r.getAs("lval_rg");
				 BigDecimal  pi_rval_rg   =(BigDecimal)r.getAs("rval_rg");
				 BigDecimal  pi_sk_agency_of_lv1   =(BigDecimal)r.getAs("sk_agency_of_lv1");
				 BigDecimal  pi_sk_region_of_lv1   =(BigDecimal)r.getAs("sk_region_of_lv1");
				   
				 v_count0 = SparkSplitUtils.getCateCode(pi_cserialno);      
				 v_count1 = SparkSplitUtils.getCateCode(pi_sk_account);
				 v_count2 = SparkSplitUtils.getCateCode(pi_sk_invpty);
				 v_count3 = SparkSplitUtils.getCateCode(pi_dk_cust_type);
				 v_count4 = SparkSplitUtils.getCateCode(pi_sk_product);
				 v_count5 = SparkSplitUtils.getCateCode(pi_sk_agency_of_lv1);
				 v_count6 = SparkSplitUtils.getCateCode(pi_sk_region_of_lv1);
				 
				 for(v_cnt0=v_count0;v_cnt0>=1;v_cnt0--)
				 {
					 for(v_cnt1=v_count1;v_cnt1>=1;v_cnt1--)
					 {
						 for(v_cnt2=v_count2;v_cnt2>=1;v_cnt2--)
						 {
							 for(v_cnt3=v_count3;v_cnt3>=1;v_cnt3--)
							 {
								 for(v_cnt4=v_count4;v_cnt4>=1;v_cnt4--)
								 {
									 for(v_cnt5=v_count5;v_cnt5>=1;v_cnt5--)
									 {
										 for(v_cnt6=v_count6;v_cnt6>=1;v_cnt6--)
										 {
											 vc_key =(v_cnt0 == 1 ?"*.*":pi_tano + '.' + pi_cserialno)+ ',' +
													 (v_cnt1 == 1 ?-1:pi_sk_account)    + ',' +
													 (v_cnt2 == 1 ?-1:pi_sk_invpty)     + ',' +
				                            	     (v_cnt3 == 1 ?"*":pi_dk_cust_type) + ',' +
				                                     (v_cnt4 == 1 ?-1: pi_sk_product )  + ',' +
				                                     (v_cnt5 == 1 ?-1:pi_sk_agency_of_lv1) + ',' +
				                                     (v_cnt6 == 1 ?-1:pi_sk_region_of_lv1);
											 innerMap.put(vc_key,ruleMap.get(vc_key));
										 } 
									 }
								 }
							 }
						 } 
					 } 
				 }// end for
		        
				 List<Row> orMap= SparkSplitUtils.reorder(innerMap);
				 double v_accu_ratio =1.0d;
	             double v_ratio =0.0d;
		         double v_lv_remain_ratio=0.0d; 
				 double v_e=0.0000001d;
				 vc_pre_rule_id =SparkSplitUtils.S_UNKNOWN;
		         Iterator<Row> it=orMap.iterator();
		         
		         Row rule=null;
		         BigDecimal priority=null;
		         String area_rule_id=null;
		         String dk_anal_dymic=null;
		         BigDecimal ratio=null;
		         
		         int v_ef=0;
		         int v_et=0;
		         int v_rule_et=0;
		         int v_tmp_val=0;
		         //int v_dtrg_no=0;
		         	   
		         List<DateRange> vt_dt_range =new ArrayList<DateRange>();
		         List<DateRange> vt_dt_range_pre  =new ArrayList<DateRange>();
		         List<DateRange>  vt_dt_range_split =new ArrayList<DateRange>();
		         vt_dt_range.add(new DateRange(pi_effective_from.intValue(),pi_effective_to.intValue(),1.0d));
		         
		         int effective_from=0;
		         int effective_to=0;
		         List<Integer> vt_array_seq=new ArrayList<Integer>();
		         while(it.hasNext())
		         {
		        	 rule=it.next();
		        	 vt_dt_range_split.clear();
		        	 effective_from=((BigDecimal)rule.getAs("effective_from")).intValue();
		        	 effective_to  =((BigDecimal)rule.getAs("effective_to")).intValue();
		        	 DateRange dr=null;
		        	 for(int i=0;i<vt_dt_range.size();i++)
		        	 {
		        		 dr=vt_dt_range.get(i);
		        		 v_ef = Math.max(dr.start_dt,effective_from);
		        		 if( effective_to < 99991231)
		        		 {
		        	          v_rule_et = SparkSplitUtils.addDays(effective_to,1);
		        		 } 
		        		 else
		        		 {    
		        			 v_rule_et = effective_to;
		        		 }
		        		 v_et = Math.min(dr.end_dt,v_rule_et);
		        		 v_accu_ratio = dr.remain_ratio;
		        	
		        	 
			        	 //只取有效日期区间段
			        	 if( v_ef<v_et && v_accu_ratio>v_e)
			        	 {
			        		 //按其它拆分维度匹配规则
			        		 if(
				        		    (pi_cserialno !=null && pi_tano !=null
				        		    &&pi_cserialno.equals(rule.getAs("cserialno"))
				        		    &&pi_tano.equals(rule.getAs("dk_tano"))
				        		    ||"*".equals(rule.getAs("cserialno"))
				        		    &&"*".equals(rule.getAs("dk_tano")) //跟oracle版本不一样，要去tano和cserialno同时有或无
				        		    )
				        		    //&& pi_lval_ag.compareTo((BigDecimal)rule.getAs("lval_ag"))>=0
				        		    //&& pi_rval_ag.compareTo((BigDecimal)rule.getAs("rval_ag"))<=0
				        		    //&& pi_lval_rg.compareTo((BigDecimal)rule.getAs("lval_rg"))>=0
				        		    //&& pi_rval_rg.compareTo((BigDecimal)rule.getAs("rval_rg"))<=0
				        		    && SparkSplitUtils.matchRule(pi_sk_account,(BigDecimal)rule.getAs("sk_account"))
				        		    && SparkSplitUtils.matchRule(pi_sk_invpty,(BigDecimal)rule.getAs("sk_invpty"))
				        		    && SparkSplitUtils.matchRule(pi_sk_prod_type,(BigDecimal)rule.getAs("sk_product_type"))
				        		    && SparkSplitUtils.matchRule(pi_sk_product,(BigDecimal)rule.getAs("sk_product"))
				        		    && SparkSplitUtils.matchRule(pi_sk_account_type,(BigDecimal)rule.getAs("sk_account_type"))
				        		    && SparkSplitUtils.matchRule(pi_sk_invpty_type,(BigDecimal)rule.getAs("sk_invpty_type"))
				        		    && SparkSplitUtils.matchRule(pi_sk_tradeacco_reg,(BigDecimal)rule.getAs("sk_tradeacco_reg"))
				        		    && SparkSplitUtils.matchRule(pi_dk_agency_type,(String)rule.getAs("dk_agency_type"))
				        		    && SparkSplitUtils.matchRule(pi_dk_cust_type,(String)rule.getAs("dk_cust_type"))
				        		    && SparkSplitUtils.matchRule(pi_agencyno,(String)rule.getAs("agencyno"))
				        		    && SparkSplitUtils.matchRule(pi_netno,(String)rule.getAs("netno"))
				        		    && SparkSplitUtils.matchRule(pi_dk_share_type,(String)rule.getAs("dk_share_type"))				       		    
			        		 )
			        		 {
			        			 priority=(BigDecimal)rule.getAs("priority");
			        			 area_rule_id=(String)rule.getAs("area_rule_id");
			        			 dk_anal_dymic=(String)rule.getAs("dk_anal_dymic");
			        			 ratio =(BigDecimal)rule.getAs("ratio");
			        			 
			        			 if(
			        					 SparkSplitUtils.nvl(vc_pre_priority).compareTo(SparkSplitUtils.nvl(priority))==0
			        				   &&SparkSplitUtils.nvl(vc_pre_rule_id).compareTo(SparkSplitUtils.nvl(area_rule_id))!=0
			        				   &&"Y".equals(vc_pre_dync_flag)
			        				   &&"Y".equals(dk_anal_dymic)
			        			   )
			        			 {
			        				 //相对比例
			        				 v_ratio = Math.max(v_lv_remain_ratio,v_accu_ratio) * ratio.doubleValue();
			        			 }
			        			 else
			        			 {
			        				 //绝对比例
			        				  v_ratio = Math.min(v_accu_ratio,ratio.doubleValue());
			        			 }	
			        			 ret.add(RowFactory.create(
			        					    r.getAs(0),//流水号，唯一值
			        					    pi_tano, 
				      	   			        pi_dk_cust_type, 
			        	   			        area_rule_id,
			        	   			        rule.getAs("dk_org_tree"),
			        	   			        rule.getAs("sk_org"),
			        	   			        rule.getAs("dk_org_tree_of_bl"),
			        	   			        rule.getAs("sk_org_of_bl"),
			        	   			        rule.getAs("dk_custmngr_type"),
			        	   			        rule.getAs("sk_invpty_of_custmngr"),			        	   			       
			        	   			        new BigDecimal(v_ef),
			        	   			        new BigDecimal(v_et),
			        	   			        null,//fa_cfm_ef
				      	   			        null,//fa_cfm_et
				      	   			        new BigDecimal(v_ratio),//ratio
				      	   			        pi_dk_system_of_sdata			        	   			     
			        	   			));
			        			 if(
			        	          //当层级发生变更时，进行层级剩余比例初始化
			        	           (vc_pre_priority==null || vc_pre_priority.compareTo( priority)!=0)
			        	           && "Y".equals(dk_anal_dymic)
			        	           )
			        	          {
			        	            //记录同层级的剩余比例
			        	            v_lv_remain_ratio = v_accu_ratio;
			        	          }
			        	          // 记录本次规则的优先级、动态比例计算标志、规则id作为下条规则的基准
			        	          vc_pre_priority  = priority;
			        	          vc_pre_rule_id   = area_rule_id;
			        	          vc_pre_dync_flag = dk_anal_dymic;
			        	          //v_accu_ratio = v_accu_ratio - v_ratio;
			        	          vt_dt_range_split.add(new DateRange(v_ef,v_et,v_accu_ratio - v_ratio));
			        	          
			        		 }//end 多维度拆分		        			 
			        	 }// end 有效时间段
		        	 }//end for
		        	 
		        	 if(vt_dt_range_split.size()>0)
		        	 {
		        		 vt_dt_range_pre.clear();
		        		 for(int m=0;m<vt_dt_range.size();m++)
		        		 {
		        			 vt_array_seq.add(vt_dt_range.get(m).start_dt);
		        			 vt_array_seq.add(vt_dt_range.get(m).end_dt);
		        		 }
		        		 for(int m=0;m<vt_dt_range_split.size();m++)
		        		 {
		        			 vt_array_seq.add(vt_dt_range_split.get(m).start_dt);
		        			 vt_array_seq.add(vt_dt_range_split.get(m).end_dt);
		        		 }
		        		 
		        		 //对数组序列从小到排序，重新构造新的日期区间集合
		        	     v_count1  = vt_array_seq.size();
		        	     for(v_cnt1=0;v_cnt1<v_count1;v_cnt1++)
		        	     {
		        	          for( v_cnt2 = v_cnt1 + 1;v_cnt2<v_count1;v_cnt2++)
		        	          {
		        	            if( vt_array_seq.get(v_cnt2) < vt_array_seq.get(v_cnt1) )
		        	            {
		        	              v_tmp_val = vt_array_seq.get(v_cnt1);
		        	              vt_array_seq.set(v_cnt1, vt_array_seq.get(v_cnt2));
		        	              vt_array_seq.set(v_cnt2,v_tmp_val);
		        	            }
		        	          }
		        	          //重新构造日期区间
		        	          if( v_cnt1 >= 1 && vt_array_seq.get(v_cnt1) > vt_array_seq.get(v_cnt1 - 1))
		        	          {
		        	            vt_dt_range_pre.add(new DateRange(vt_array_seq.get(v_cnt1 - 1),vt_array_seq.get(v_cnt1),0.0d));		        	           
		        	          }
		        	      } //end for v_cnt1
		        	     
		        	     //计算新构造日期区间的剩余分摊比例
		        	        //v_count1  := vt_dt_range_pre.count;
		        	        //v_dtrg_no := 1;
		        	        for (v_cnt1=0;v_cnt1<vt_dt_range_pre.size(); v_cnt1++)
		        	        {
		        	            //v_count2 := vt_dt_range_split.count;
		        	           int v_found2 = 0;
		        	            //优先在本次拆分新拆分日期区间内查找剩余比例
		        	            for(v_cnt2 =0;v_cnt2<vt_dt_range_split.size(); v_cnt2++)
		        	            {
		        	              if( vt_dt_range_pre.get(v_cnt1).start_dt >= vt_dt_range_split.get(v_cnt2).start_dt
		        	                  && vt_dt_range_pre.get(v_cnt1).start_dt < vt_dt_range_split.get(v_cnt2).end_dt
		        	                )
		        	              {
		        	                vt_dt_range_pre.get(v_cnt1).remain_ratio = vt_dt_range_split.get(v_cnt2).remain_ratio;
		        	                v_found2 = 1;
		        	                break;
		        	              }
		        	            }
		        	            //若找不到，则在上次拆分日期区间内查找剩余比例
		        	            if( v_found2 == 0 )
		        	            {
		        	              //v_count2 := vt_dt_range.count;
		        	              for( v_cnt2 =0;v_cnt2< vt_dt_range.size();v_cnt2++)
		        	              {
		        	                if( vt_dt_range_pre.get(v_cnt1).start_dt >= vt_dt_range.get(v_cnt2).start_dt
		        	                 && vt_dt_range_pre.get(v_cnt1).start_dt < vt_dt_range.get(v_cnt2).end_dt
		        	                   )
		        	                {
		        	                  vt_dt_range_pre.get(v_cnt1).remain_ratio = vt_dt_range.get(v_cnt2).remain_ratio;
		        	                  v_found2 = 1;
		        	                  break;
		        	                }
		        	              }
		        	            }
		        	         //20170220 louyk modify 数组元素删除后，数组下标不连续的问题开始编号问题
		        	        }
		        	        vt_dt_range.clear();
		        		 
		        	        //20161122 suncj modify 解决数组元素删除后，数组下标不从1开始编号问题
		        	        //vt_dt_range:=vt_dt_range_pre;
		        	        if( vt_dt_range_pre.size() > 0)
		        	        {
		        	          int v_j = 0;
		        	          for(int v_i=0;v_i< vt_dt_range_pre.size();v_i++)
		        	          {
		        	            //20170220 louyk modify 数组元素删除后，数组下标不连续的问题开始编号问题,使用IF过滤剩余占比为0的记录
		        	            if( vt_dt_range_pre.get(v_i).remain_ratio > v_e)
		        	            {
		        	              vt_dt_range.add(v_j, vt_dt_range_pre.get(v_i));//TODO set or add
		        	              v_j = v_j + 1;
		        	            }//end if
		        	          }//end for
		        	 		}//end if
		        	 }// end if cnt2>0
		        	 if(vt_dt_range.size()==0)
		        	 {
		        		 break;
		        	 }
		         }// end while
				 
		         //处理拆分剩余比例
		          
		         for( v_cnt1=0;v_cnt1<vt_dt_range.size(); v_cnt1++)
		         {  
		        	 ret.add(RowFactory.create(
     					    r.getAs(0),//流水号，唯一值
     	   			        pi_tano,//dk_tano
     	   			        pi_dk_cust_type,//dk_org_tree
     	   			        null,//area_rule_id
     	   			        null,
     	   			        new BigDecimal(-1),//sk_org
     	   			        null,//dk_org_tree_of_bl
     	   			        new BigDecimal(-1),//sk_org_of_bl
     	   			        null,//dk_custmngr_type
     	   			        new BigDecimal(-1),//sk_invpty_of_custmngr     	   			        
     	   			        new BigDecimal(vt_dt_range.get(v_cnt1).start_dt),//effective_from
     	   			        new BigDecimal(vt_dt_range.get(v_cnt1).end_dt),//effective_to
     	   			        null,//fa_cfm_ef
     	   			        null,//fa_cfm_et
     	   			        new BigDecimal(vt_dt_range.get(v_cnt1).remain_ratio),//ratio
     	   			        pi_dk_system_of_sdata
     	   			));
		         }
				return ret;
			}
		});
		
		JavaRDD<Row> x=w.flatMap(new FlatMapFunction<List<Row>,Row>(){
			private static final long serialVersionUID = -6111647437563220114L;
			@Override
			public Iterator<Row> call(List<Row> arg0) throws Exception {
					return arg0.iterator();
			}
	    });
		//hc.sql("truncate table "+schema+"."+pi_table);
		StructType st =  hc.read().parquet("/Users/Jack/Documents/workspace/spark2/src/org/hesc/example/parquet/ass_ta_shr_arsplt").schema();	
		
		hc.createDataFrame(x, st).write()
	    .format("parquet") 
	    .mode( SaveMode.Append ).save("/Users/Jack/Documents/workspace/spark2/src/org/hesc/example/parquet/ass_ta_shr_arsplt2");
	
	
		 
		hc.close();
	}
}
