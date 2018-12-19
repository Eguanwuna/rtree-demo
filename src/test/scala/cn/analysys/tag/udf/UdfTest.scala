package cn.analysys.tag.udf


/**
  * Created by l on 17-9-15.
  */
object UdfTest { // extends FlatSpec

  val r = new Random()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UserBehiviorSparksql")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.default.parallelism", 4)
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //.enableHiveSupport()
      .getOrCreate()
   // datagenetor(spark)
    testDataClusterCal(spark)
   // test2(spark)
  //  parseDoubleTest

  }


  def getDoulbe(s:String):Double={

    var d = 0.0

    try {
      d = s.toDouble

    }catch{
      case x:Throwable =>
    }

    d
  }



  def parseDoubleTest={

    val aa = "NaN"
    val bb = "0.001"
    val cc = ""
    println(getDoulbe(aa))
    println(getDoulbe(bb))
    println(getDoulbe(cc))


    println(aa.toDouble)
    println(bb.toDouble)
    println(cc.toDouble)

  }

  def randomvalue =   r.nextInt(100)*0.0001

  def datagenetor(spark:SparkSession) ={

    val  data = Array((34.12,112.1231),( 35.44,114.3434),(37.45,115.45)
      ,( 39.12,120.45),(30.56,100.123),(33.23,106.34),(33.34,106.74),(33.45,106.84),(34.121,112.12311),(33.451,106.841))
    val right = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,4,
      5,5,5,6,6,6,6,6,6,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,8,9)
    val rightsize = right.size-1
    val dataArray = new  ArrayBuffer[(Double,Double)]
    for(i <- 0 to 10000)  dataArray +=  {
       val seedData = data(right(r.nextInt(rightsize)))
      (seedData._1+randomvalue,seedData._2+randomvalue)
    }


    val df = dataArray.toDF().withColumnRenamed("_1","lat")
                             .withColumnRenamed("_2","lon")


    df.registerTempTable("testtable")

    spark.sql("select * from testtable limit 10").show(false)


    df.write.format("parquet").save("/Users/yunzhi.liao/git/nbdata/fangzhou-tag/analysysTAG/data/testdata")



  }




  def testDataClusterCal(spark:SparkSession): Unit ={

    var data = spark.read.parquet("/Users/yunzhi.liao/git/nbdata/fangzhou-tag/analysysTAG/data/testdata")
    data.cache()
    data.registerTempTable("testtable")
    spark.sql("select * from testtable limit 10").show()
    spark.sql("select count(1) from testtable ").show()


    //spark.udf.register("lngLatClusterCal", new cn.analysys.tag.udaf.LngLatClusterCal)
    //spark.sql("select   lngLatClusterCal(lat,lon) as carray from testtable ").show(false)

    val latArray  = spark.sql("select   lat from testtable  ").collect().map(r => r.getAs[Double]("lat")  )
    val lonArray  =  spark.sql("select   lon from testtable  ").collect().map(r => r.getAs[Double]("lon")   )

    //println(LonLatKmeans.kmeansCompute(lonArray,latArray))

    val b = System.currentTimeMillis()
    for(i <-0 to 500) println(s" i : $i  ,value:  ${LonLatKmeans.kmeansCompute(lonArray,latArray)}")
    println(System.currentTimeMillis()-b)


  }





  def test2(spark:SparkSession) ={

//    var data = spark.read.parquet("./tag_bds_fill_lbs_info_new")
//    data.cache()
//    data.registerTempTable("tag_bds_org_agg_lbs_info_new")
//    spark.sql("select * from tag_bds_org_agg_lbs_info_new limit 10").show()


//    spark.udf.register("lngLatClusterCal", new cn.analysys.tag.udaf.LngLatClusterCal)
//
//    spark.sql("select   imei, \nlngLatClusterCal(cast(lon as double),cast(lat as double)) as carray\nfrom tag_bds_org_agg_lbs_info_new\ngroup by imei").show(false)



    spark.udf.register("getDateTypeOfDay", new cn.analysys.tag.udf.getDateTypeOfDay,IntegerType)
    spark.udf.register("getPeriodOfDay", new cn.analysys.tag.udf.getPeriodOfDay,StringType)

    spark.sql("select getDateTypeOfDay(1231231232l) ").show(false)


  //  getDateTypeOfDay,cn.analysys.tag.udf.getDateTypeOfDay,IntegerType
  //  getPeriodOfDay,cn.analysys.tag.udf.getPeriodOfDay,StringType



  }








  def test1(spark:SparkSession) = {


    var data = spark.read.parquet("./tag_bds_org_agg_lbs_info")
    data.cache()
    data.registerTempTable("tag_bds_org_agg_lbs_info")

    spark.udf.register("fillLbsUDF", new cn.analysys.tag.udf.tag.FillLbsUDF(), StringType)

    var sql = "select gps" +
      "        from ( select imei, fillLbsUDF(cast(lon as string), cast(lat as string), wifi, basestation, ip) as gps " +
      "  from tag_bds_org_agg_lbs_info      limit 10     ) a  where a.gps != \"@@\""

    //  spark.sql(sql).show(false)
    //  spark.sql(sql).show(false)

    sql = "select * from  tag_bds_org_agg_lbs_info      limit 15"

    spark.sql(sql).show(false)


    sql = "select gps" +
      "        from ( select imei, fillLbsUDF(cast(lon as string), cast(lat as string), wifi, basestation, ip) as gps " +
      "  from tag_bds_org_agg_lbs_info      limit 15     ) a  where a.gps != \"@@\""

    spark.sql(sql).show(false)



    // spark.sql(sql).show(false)

    //  sql = "select gps" +
    //    "        from ( select imei, fillLbsUDF(cast(lon as string), cast(lat as string), wifi, basestation, ip) as gps " +
    //    "  from tag_bds_org_agg_lbs_info      limit 20     ) a  where a.gps != \"@@\""
    //
    //  spark.sql(sql).show(false)
    //
    //  spark.sql(sql).show(false)
    //
    //
    //
    //
    //  spark.sql("select * from tag_bds_org_agg_lbs_info limit 10").show(false)


    spark.udf.register("fillLbsUDAF", new cn.analysys.tag.udf.tag.FillLbsUDAF())


    sql = "select gps " +
      "     from ( select  fillLbsUDAF(cast(lon as string), cast(lat as string), wifi, basestation, ip,imei,cast(ct as string)) as gps " +
      "     from  ( select * from  tag_bds_org_agg_lbs_info      limit 15  )b   ) a "


    spark.sql(sql).show(false)


    println("********************************************")

    sql = "select gps_carray.lon,gps_carray.lat,gps_carray.caltype,gps_carray.imei,gps_carray.ct\nfrom ( select  fillLbsUDAF(cast(lon as string), cast(lat as string), wifi, basestation, ip,imei,cast(ct as string)) as gps \nfrom  ( select * from  tag_bds_org_agg_lbs_info      limit 15  )b   ) a  lateral view explode(gps) t as gps_carray "


    spark.sql(sql).show(false)


    sql = "select * from  tag_bds_org_agg_lbs_info      limit 15"

    spark.sql(sql).show(false)


    //  val MINUTE = "minute"
    //  val HOUR = "hour"
    //  val DAY = "day"
    //  val WEEK = "week"
    //  val MONTH = "month"
    //  val QUAETER = "quarter"
    //  val YEAR = "year"

    //  spark.sql("select formatTimeByUnit('2018-03-20 10:11:12','minute') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-03-20 10:11:12','hour') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-03-20 10:11:12','day') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-03-25 10:11:12','week') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-03-20 10:11:12','month') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-04-20 10:11:12','quarter') ").show(100,false)
    //  spark.sql("select formatTimeByUnit('2018-03-20 10:11:12','year') ").show(100,false)


    //spark.sql("select milliToStringByUnit(1521515373377,'minute') ").show(100,false)


    /*
  *
  * getUuid,cn.analysys.udf.common.GetUuid,StringType
  funnelsum,cn.analysys.udf.funnel.FunnelSum
  funnelmergecount,cn.analysys.udf.funnel.FunnelMergeEventTimeAndIDCount
  dataFetchSample,cn.analysys.udf.common.DataFetchSample,BooleanType
  jsoninfogetstring,cn.analysys.udf.common.JsonInfoGetString,StringType
  jsoninfogetlong,cn.analysys.udf.common.JsonInfoGetLong,LongType
  jsoninfogetdouble,cn.analysys.udf.common.JsonInfoGetDouble,DoubleTypea

  formatTimeByUnit,cn.analysys.udf.common.FormatTimeByUnit,StringType
  milliToStringByUnit,cn.analysys.udf.common.MilliToStringByUnit,StringType
  createCrowd,cn.analysys.udf.crowd.CreateCrowd
  createCrowdHllc,cn.analysys.udf.crowd.CreateCrowdHllc
  crowdContain,cn.analysys.udf.crowd.CrowdContain,StringType

  *
  * */


    //spark.sql("select * from UserBehavior limit 100").show(100,false)
    //  spark.udf.register("jsoninfogetstring",new JsonInfoGetString,StringType)
    //  spark.udf.register("jsoninfogetlong",new JsonInfoGetLong,LongType)
    //  spark.udf.register("jsoninfogetdouble",new JsonInfoGetDouble,DoubleType)
    //  spark.udf.register("dataFetchSample",new DataFetchSample,BooleanType)
    //  spark.udf.register("funnelmergecount", new FunnelMergeEventTimeAndIDCount)
    //  spark.udf.register("funnelsum",new FunnelSum)

    //  spark.sql("select * from h5 ")   .show(10)
    //
    //  spark.sql("select formatTimeByUnit(statistic_time,'minute'),statistic_time,milliToStringByUnit(page_etime,'minute'),page_etime  from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'hour'),statistic_time,milliToStringByUnit(page_etime,'hour'),page_etime  from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'day'),statistic_time,milliToStringByUnit(page_etime,'day'),page_etime  from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'week'),statistic_time,milliToStringByUnit(page_etime,'week'),page_etime  from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'month'),statistic_time,milliToStringByUnit(page_etime,'month'),page_etime  from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'quarter') ,statistic_time,milliToStringByUnit(page_etime,'quarter'),page_etime from h5 ").show(2,false)
    //  spark.sql("select formatTimeByUnit(statistic_time,'year'),statistic_time,milliToStringByUnit(page_etime,'year'),page_etime  from h5 ").show(2,false)

    // spark.udf.register("createCrowd",new cn.analysys.udf.crowd.CreateCrowdMini)
    // spark.udf.register("createCrowdHuge",new cn.analysys.udf.crowd.CreateCrowdHuge)
    // spark.udf.register("crowdContain",new cn.analysys.udf.crowd.CrowdContain,IntegerType)
    // spark.udf.register("crowdContainLong",new cn.analysys.udf.crowd.CrowdContainLong,IntegerType)

    //
    //  spark.udf.register("uVCalHuge",new cn.analysys.udf.keep.KeepCalHuge)
    // spark.udf.register("uVCalMini",new cn.analysys.udf.keep.KeepCalMini)
    // spark.udf.register("uVCombineCal",new cn.analysys.udf.keep.KeepCombineCal)


    //  spark.udf.register("crowdContain",new cn.analysys.udf.crowd.CrowdContain,IntegerType)

    //  spark.sql("select createCrowd(uuid,'uuid','crowdid_appid')  AS uv  from h5").show(10)
    //  spark.sql("select createCrowd(tmp_id,'device','crowdid_appid')  AS uv  from h5").show(10)
    //  spark.sql("select count(distinct uuid) from h5 where crowdContainLong('crowdid_appid','uuid',uuid) = 1 ").show(10)
    //  spark.sql("select count(distinct tmp_id) from h5 where crowdContain('crowdid_appid','device',tmp_id) = 1").show(10)


    // spark.sql("select  uv.uuid_uv,uv.device_uv from (    select createCrowdMini(uuid,tmp_id,'crowdid_appid')  AS uv  from h5 ) t    ").show(10)
    // spark.sql("select count (distinct uuid)  as uuiduv from h5     ").show(10)
    // spark.sql("select count (distinct tmp_id)  as tmp_id_uv from h5     ").show(10)
    // spark.sql("select * from h5   ").show(10)
    //(crowdID: String, keyType: String, uuid: String)
    // spark.sql(" select count(distinct uuid) from h5 where crowdContain('crowdid_appid','uuiduv',uuid) = 1  ").show(10)
    // spark.sql(" select count(distinct tmp_id) from h5 where crowdContain('crowdid_appid','deviceuv',tmp_id) = 1  ").show(10)

    // val datattt =
    //   spark.sql("select  uVCombineCal(app_id,hllcstr) from   " +
    //    "  (select app_id, uVCalMini(uuid) as hllcstr   from h5  group by app_id) t ").show(1,false)


    // spark.sql(" select app_id, uVCalMini(uuid) as hllcstr   from h5  group by app_id) t ").show(10)
    // spark.udf.register("funnelMerge",new cn.analysys.udf.funnel.FunnelMerge)
    // spark.udf.register("funnelSum",new cn.analysys.udf.funnel.FunnelSum)

    //spark.sql(" select standard_event_id,page_stime from h5  ").show()


    //  spark.sql(" select standard_event_id,count(1) from h5 group by  standard_event_id ").show()
    //
    //  spark.udf.register("funnelMerge",new cn.analysys.udf.funnel.FunnelMerge)
    //  spark.udf.register("funnelSum",new cn.analysys.udf.funnel.FunnelSum)


    //  StructField("item", StringType, true) ::
    //  StructField("eventTimestamp", IntegerType, true) ::
    //  StructField("windowLength", IntegerType, true) ::Nil)


    //
    //  var sql = "select funnelSum(funneldata) from  (" +
    //    "select uuid,funnelMerge(concat_ws('_', case when standard_event_id in (85614,85568) then '0' end , case  when  standard_event_id in (85568)  then '1' end ,      case  when  standard_event_id in (133135,85614) then '2' end)  as step ,page_stime ,1000000)  as funneldata " +
    //    "from h5  where  standard_event_id in (85614,85568,133135) group by uuid " +
    //    ") t "
    //
    //
    //  sql = " select funnelSum(funneldata) from  " +
    //    "(select uuid,funnelMerge(" +
    //    "concat_ws('_',  case when standard_event_id in (85614,85568) then '0' end , " +
    //    " case  when  standard_event_id in (85568)  then '1' end , " +
    //    " case  when  standard_event_id in (133135,85614) then '2' end)  behivior_id ,page_stime ,1000000)  funneldata" +
    //    " from h5 where standard_event_id in (85614,85568,133135) group by uuid ) temp2"
    //
    //
    //  sql = " select uuid," +
    //    "funnelMerge(concat_ws('_',  case when standard_event_id in (85614,85568) then '0' end ,  " +
    //    "case  when  standard_event_id in (85568)  then '1' end ,  " +
    //    "case  when  standard_event_id in (133135,85614) then '2' end)  as  behivior_id ,page_stime ,1000000) as   funneldata " +
    //    "from h5 where standard_event_id in (85614,85568,133135) group by uuid"


    /*
*
* +-----------------+--------+
|standard_event_id|count(1)|
+-----------------+--------+
|           133135|     198|
|           135375|      29|
|            48331|       8|
|            85874|       9|
|           135099|      47|
|           112171|     221|
|            92563|       2|
|            85614|     138|
|            85568|     155|
|           135260|      19|
|           134259|      29|
|            85511|       7|
|            85419|      17|
|           135143|       4|
|           135290|       1|
|            92364|      21|
|           136572|       2|
|           135411|       1|
85614  85614
* */

    //  spark.udf.register("keepKeyRelation",new cn.analysys.udf.keep.KeepKeyRelation)
    //  spark.udf.register("keepRelationMerge",new cn.analysys.udf.keep.KeepRelationMerge)
    //  spark.udf.register("channelKeepKeyRelation",new cn.analysys.udf.keep.ChannelKeepKeyRelation)
    //  spark.udf.register("cycleKeepKeyRelation",new cn.analysys.udf.keep.CycleKeepKeyRelation)
    //
    // // sql = " select funnelSum(funneldata) from (select uuid, funnelMerge(concat_ws('_', case  when  standard_event_id in (85568)  then '0' end ,  case  when  standard_event_id in (85568) then '1' end) ,(page_stime/1000-1450000000 ) ,100000000) funneldata from h5  group by uuid)t"
    // // println(sql)
    //
    //  sql = " select uuid, keepKeyRelation(10,  (case  " +
    //    "when  standard_event_id in (85614)  then '2' " +
    //    "when  standard_event_id in (85568)  then '3' " +
    //    " else 1  end),1,1) from   h5  where  standard_event_id in (85614,85568)  group by uuid     "
    //
    //  sql = "   select keepRelationMerge(relation)  from (select uuid, cycleKeepKeyRelation((case  " +
    //    "when  standard_event_id in (85614)  then '0' " +
    //    "when  standard_event_id in (85568)  then '2' " +
    //    " else 1  end),1,1) as relation from   h5  where  standard_event_id in (85614,85568)  group by uuid) temp   "
    //
    //
    // //  sql = "  select event.uuid,channelKeepKeyRelation(10,30,1) as relation from h5 event where event.day = '201803'  group by  event.uuid limit 10 "
    //
    //  println(sql)
    //  spark.sql(sql).show(10000,false)
    //  spark.stop()

    // |            85614|     138|
    // |            85568|     155|


    //    spark.sql("select * from UserBehavior  where record_date in ('20170101','20170102','201701003') " +
    //      " and behivior_id in ( 10001,10004)   and (behivior_id != 10004 or ( jsoninfogetstring(behivior_pop,'price') > 5000 and    jsoninfogetdouble(behivior_pop,'price') < 6000   " +
    //     // "and jsoninfogetstring(behivior_pop,'brand') = 'Apple'
    //      ")) " +
    //      "" +
    //      " limit 10").show(100,false)

    //  benchTest("count:","select count(1) from  UserBehavior  " +
    //    "where   record_date in ('20170101','20170102')" )
    //

    //    var sql ="select funnelsum(funneldata) from  ( select user_id,funnelmergecountindex(behivior_id,event_time) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecountindex",sql)

    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10004',1000000,1451577600) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecount",sql)
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecountindex(behivior_id,event_time) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecountindex",sql)
    //
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10004',1000000,1451577600) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecount",sql)
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecountindex(behivior_id,event_time) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecountindex",sql)
    //
    //
    //  var sqlorg = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10007',1000000,1451577600) as funneldata  " +
    //    "from UserBehavior  where   behivior_id in (10001,10002,10003,10007)  and   record_date in ('20170101','20170102')  " +
    //    "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //    " group by  user_id ) temp2"
    //  println(sqlorg)
    //  benchTest("funnelmergecount",sqlorg)
    //
    //  val  sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10007',1000000,1451577600) as funneldata  " +
    //    "from UserBehavior  where   behivior_id in (10001,10002,10003,10007) " +
    //    "and dataFetchSample(user_id,64,1)  " +
    //    "and record_date in ('20170101','20170102')  " +
    //    "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //    " group by  user_id ) temp2"
    //  println(sql)
    //  benchTest("funnelmergecount  ",sql)


    //
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecountindex(behivior_id,event_time) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104','20170105','20170106') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecountindex",sql)
    //
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10004',1000000,1451577600) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104','20170105','20170106') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecount",sql)
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecountindex(behivior_id,event_time) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104','20170105','20170106') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecountindex",sql)
    //
    //
    //    sql = "select funnelsum(funneldata) from  ( select user_id,funnelmergecount(behivior_id,event_time,'10001,10002,10003,10004',1000000,1451577600) as funneldata  " +
    //      "from UserBehavior  where   behivior_id in (10001,10002,10003,10004)  and   record_date in ('20170101','20170102','20170103','20170104','20170105','20170106') " +
    //      "and  (  behivior_id != 10004 or  jsoninfogetstring(behivior_pop,'brand') = 'Apple' )  " +
    //      " group by  user_id ) temp2"
    //    benchTest("funnelmergecount",sql)
    //  def benchTest(name:String,sql:String): Unit ={
    //    val b2 = System.currentTimeMillis()
    //    spark.sql(sql).show(10,false)
    //    val e2 = System.currentTimeMillis()
    //    println(s"name $name ,  cost:  ${e2-b2}")
    //  }


  }


}

