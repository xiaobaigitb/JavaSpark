package cn.com.businessmatrix.hive;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*
 *  金额分段
 *
 *  参数顺序：
 *
 *
 *
 */
public class SparkTmpIdxFundbalD2Hive {


    public static void main(String[] args) throws Exception {

        String sparktable = args[0];//存放spark临时数据的表
        String startdate = args[1];
        String enddate = args[2];
        String startdate_pre = args[3];
        String schema_ods = args[4];
        String schema_pub = args[5];
        String schema_lbl = args[6];
        String sparkname = args[7];//spark任务名称，方便查找
        String beforedate = args[8];

        String savemode = "append";
        if (args.length > 9)
            savemode = args[9];


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
                .appName(sparkname)
                //.master("local[1]")
                //.master("yarn-cluster")
                .getOrCreate();

        String sql_lookdf = "select * from " + schema_lbl + "." + sparktable + " where 1=2 ";
        // hc.sql("alter table "+schema_lbl+"."+sparktable+" drop if exists partition(bus_date>="+startdate_pre+",bus_date<="+enddate+")");
        System.out.println("alter table " + schema_lbl + "." + sparktable + " drop if exists partition(bus_date>=" + startdate_pre + ",bus_date<=" + enddate + ")");

        String sql = " select " +
                " dk_cust_type,   " +
                " pre_bus_date,   " +
                " next_bus_date,  " +
                " cfm_date, " +
                " sk_product, " +
                " bk_product, " +
                " product_short_name, " +
                " dk_sex, " +
                " dk_age_range, " +
                " sk_region,  " +
                " dk_open_age_range, " +
                " null as sk_agency, " +
                " null as agencyno, " +
                " null as agency_branch_name,   " +
                " cast(sum(shares) as DECIMAL(21,2)) as shares, " +
                " cast(sum(all_capital) as DECIMAL(21,6)) as all_capital, " +
                " dk_amount_range,  " +
                " cast(sum(capital) as DECIMAL(21,6)) as capital, " +
                " bus_date " +
                " from ( " +
                " select /*+ BROADCAST(r),BROADCAST(b),BROADCAST(t3) */ " +
                " nvl(t1.dk_cust_type,'-1') as dk_cust_type,  " +
                " cast(substr(regexp_replace(cast(date_sub(from_unixtime(unix_timestamp(cast(t1.bus_date as string),'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'), 1) as string),'-',''),1,8) as decimal(8,0)) as pre_bus_date,  " +
                " cast(substr(regexp_replace(cast(date_sub(from_unixtime(unix_timestamp(cast(t1.bus_date as string),'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'),-1) as string),'-',''),1,8) as decimal(8,0)) as next_bus_date,  " +
                " t1.cfm_date, " +
                " t1.sk_product, " +
                " t1.bk_product, " +
                " t1.product_cname as product_short_name, " +
                " case when t1.dk_cust_type ='1' and t2.dk_sex in('F','M') then t2.dk_sex " +
                "  else '-1' " +
                " end as dk_sex, " +
                "  case when nvl(t2.age,-1)=-1  then 'AGE00' " +
                "   when t2.age<20  then 'AGE01' " +
                "   when t2.age<=30 then 'AGE02' " +
                "  when t2.age<=40 then 'AGE03' " +
                "  when t2.age<=50 then 'AGE04' " +
                "  when t2.age<=60 then 'AGE05' " +
                "  else 'AGE06' " +
                "  end as dk_age_range, " +
                " case when r.sk_region not in(12754,14571,11884,14426,10603) and r.sk_region is not null then t3.sk_region_of_lv2  " +
                "  when r.sk_region in(12754,14571,11884,14426,10603) then r.sk_region   " +
                "  else -1 " +
                "  end as sk_region,  " +
                "  case when p.open_date is null or p.open_date=19000101 then 'ACC00' " +
                "  when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<1 then 'ACC01' " +
                "  when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<=3 then 'ACC02' " +
                "  when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<=6 then 'ACC03' " +
                "  when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<=12 then 'ACC04' " +
                " when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<=36 then 'ACC05' " +
                "  when datediff(from_unixtime(unix_timestamp(cast(t1.cfm_date as string),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(cast(p.open_date as string),'yyyyMMdd'),'yyyy-MM-dd'))/30<=60 then 'ACC06' " +
                "  else 'ACC07' " +
                "  end  as dk_open_age_range, " +
                "  null as sk_agency, " +
                "  null as agencyno, " +
                " null as agency_branch_name, " +
                " shares as shares, " +
                " all_capital as all_capital, " +
                "  case " +
                " when t1.all_capital <= 10000 then   'CAP01' " +
                " when t1.all_capital <= 50000 then   'CAP02' " +
                " when t1.all_capital <= 100000 then  'CAP03' " +
                " when t1.all_capital <= 200000 then  'CAP04' " +
                " when t1.all_capital <= 500000 then  'CAP05' " +
                " when t1.all_capital <= 1000000 then 'CAP06' " +
                " when t1.all_capital <= 5000000 then 'CAP07' " +
                " else 'CAP08' " +
                " end  as dk_amount_range,  " +
                " capital as capital, " +
                " t1.bus_date " +
                " from " + schema_pub + ".flt_ass_cust_fundbal t1  " +
                " left join (select  " +
                "   q1.sk_invpty_of_cust, " +
                " 	q2.dk_sex, " +
                "   cast(case when  q2.birthday is null or q2.birthday<'18000101' or q2.birthday>'" + enddate + "'  then -1  " +
                "      when  cast(substr(cast(q1.bus_date as string),5,4) as int)-cast(substr(q2.birthday,5,4) as int)>=0  " +
                "     then cast(substr(cast(q1.bus_date as string),1,4) as int)-cast(substr(q2.birthday,1,4) as int)  " +
                "   else cast(substr(cast(q1.bus_date as string),5,4) as int)-cast(substr(q2.birthday,1,4) as int)-1  " +
                "   end as int)  as age,  " +
                "   null as cfm_date, " +
                "  q2.dk_id_type,  " +
                "  q2.std_idno,   " +
                "  q2.dk_cust_type, " +
                "  substr(q2.std_idno,1,4) as bk_region, " +
                "  q1.bus_date " +
                "  from (select t1.sk_invpty_of_cust,t1.bus_date from " + schema_pub + ".flt_ass_cust_fundbal t1 " +
                "   where  t1.bus_date between cast('" + startdate_pre + "' as int) and " + enddate + "  " +
                "   and t1.bus_date_m between cast(substr('" + startdate_pre + "',1,6) as int)  " +
                "    and cast(substr('" + enddate + "',1,6) as int) " +
                "   and t1.dk_cust_type ='1' " +
                "   group by t1.sk_invpty_of_cust,t1.bus_date " +
                "     ) q1 " +
                "  left join " + schema_pub + ".flt_ip_custinfo q2 " +
                "      on q1.sk_invpty_of_cust=q2.sk_invpty_of_cust " +
                "     and q2.bus_date=" + beforedate + "    " +
                "    ) t2   " +
                " on t1.sk_invpty_of_cust=t2.sk_invpty_of_cust and t1.bus_date=t2.bus_date " +
                " left join (select bk_region_of_src,sk_region from " + schema_ods + ".comm_region_mapp where dk_system_of_sdata='DC') r   " +
                "  		 on t2.dk_id_type='1-0'   and r.bk_region_of_src=t2.bk_region   " +
                " left join (select sk_region,sk_region_of_lv2 from " + schema_ods + ".comm_region_hierarchy where dk_region_tree='02') t3  " +
                "   			on r.sk_region=t3.sk_region " +
                " left join (select sk_invpty_of_cust  " +
                "   ,min(nvl(open_date,19000101)) as open_date   " +
                "   from " + schema_pub + ".flt_agrm_fundaccount " +
                "   where bus_date=" + beforedate + "  " +
                "   group by sk_invpty_of_cust " +
                " ) p  " +
                " on t1.sk_invpty_of_cust =p.sk_invpty_of_cust " +
                " left join " + schema_pub + ".flt_prod_product b  " +
                "  on t1.sk_product = b.sk_product " +
                "  and b.bus_date=" + beforedate + " " +
                " where t1.bus_date between cast('" + startdate_pre + "' as int) and " + enddate + "  " +
                "  and t1.bus_date_m between cast(substr('" + startdate_pre + "',1,6) as int) " +
                "      and cast(substr('" + enddate + "',1,6) as int) " +
                " and b.dk_product_type in('01','02')  " +
                " ) w " +
                " group by " +
                " dk_cust_type,  pre_bus_date,  next_bus_date,  cfm_date, sk_product,bk_product, " +
                " product_short_name, dk_sex, dk_age_range, sk_region,  dk_open_age_range," +
                //" sk_agency, agencyno, agency_branch_name, " +
                " dk_amount_range,   bus_date ";

        System.out.println(sql_lookdf);
        System.out.println();
        System.out.println(sql);
        System.out.println();

        Dataset<Row> lookdf = hc.sql(sql_lookdf);
        Dataset<Row> adf = hc.sql(sql);

        hc.createDataFrame(adf.javaRDD(), lookdf.schema()).write()
                .format("hive")
                .partitionBy("bus_date")
                .mode("append".equalsIgnoreCase(savemode) ? SaveMode.Append : SaveMode.Overwrite)
                .saveAsTable(schema_lbl + "." + sparktable);


        hc.close();

    }

}
