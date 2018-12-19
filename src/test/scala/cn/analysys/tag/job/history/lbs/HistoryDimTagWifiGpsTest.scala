package cn.analysys.tag.job.history.lbs

object HistoryDimTagWifiGpsTest {
  //  val FORMAT_DATA = "yyyyMMdd"
  //  private val conf = HBaseConfiguration.create
  //  conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk14")
  //  conf.set("hbase.zookeeper.property.clientPort", "2181")
  //  conf.set("zookeeper.znode.parent", "/hbase")
  //  conf.set("zookeeper.recovery.retry", "10")
  //
  //  val HBASE_FAMILY_NAME = "i"
  //  val HBASE_ROWKEY_SPLIT = ":"
  //
  //  val HBASE_TABLE_BS_GPS = new HTable(conf, "dim_tag_base_station_gps")
  //
  //  val HBASE_GPS_WIFI_REGION_NUM: Int = 100
  //  val HBASE_GPS_BS_REGION_NUM: Int = 20

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[2]").getOrCreate()

    val data = spark.sparkContext.textFile("E:\\Code\\Yi_Git\\analysysTAG\\src\\test\\resources\\gps_wifi_demo.txt")
      .filter(_.split("\t").length == 3)
      .map(line => {
//      val gpl = line.split("\t")(0)
//      val wifiarr = line.split("\t")(2).split("\\|").filter(!Strings.isNullOrEmpty(_))
//      val wifinamearr = line.split("\t")(1).split("\\|").filter(!Strings.isNullOrEmpty(_))
//      val wifiBuffer = ListBuffer[Tuple2[String,String]]()
//      if (wifiarr.length >= wifinamearr.length){
//        for (i <- wifinamearr.indices) {
//          val wifiname = wifinamearr(i)
//          if (!filterPublicWiFi(wifiname)) {
//            wifiBuffer.+=((wifiarr(i), wifinamearr(i)))
//          }
//        }
//      }
      Row(line.split("\t")(0), line.split("\t")(1), line.split("\t")(2))
    })

    val sourceType = StructType(Array(
      StructField("gpl", DataTypes.StringType, nullable = true),
      StructField("wifi_name", DataTypes.StringType, nullable = true),
      StructField("wifi_mac_address", DataTypes.StringType, nullable = true)
    ))

    spark.sqlContext.createDataFrame(data, sourceType).createOrReplaceTempView("tmp_source_wifi")

    spark.udf.register("wifiUDF", (wifiName:String, wifiMac:String) => {
      val wifiarr = wifiMac.split("\\|").filter(!Strings.isNullOrEmpty(_))
      val wifinamearr = wifiName.split("\\|").filter(!Strings.isNullOrEmpty(_))
      val wifiBuffer = ListBuffer[Tuple2[String, String]]()
      if (wifiarr.length >= wifinamearr.length) {
        for (i <- wifinamearr.indices) {
          val wifiname = wifinamearr(i)
          if (!filterPublicWiFi(wifiname)) {
            wifiBuffer.+=((wifiarr(i), wifinamearr(i)))
          }
        }
      }
      wifiBuffer.map(_._1).mkString("|")
    })

    val dataframe = spark.sql(s"select gpl, wifiUDF(wifi_name, wifi_mac_address) from tmp_source_wifi where 1=1 " +
      s"and gpl <> 'null' and gpl <> '' and gpl <> 'null' and gpl <> '0.0-0.0' " +
      s"and split(gpl, '-')[1] <> 'null' and split(gpl, '-')[1] <> '' " +
      s"and split(gpl, '-')[0] <> 'null' and split(gpl, '-')[0] <> '' " +
      s"and wifi_mac_address <> 'null'")
        .toDF("gpl", "wifi")

    dataframe.show(false)

    val flatData = dataframe.rdd.flatMap(row => {
      val arr = ListBuffer[String]()
      val gpl = row.getAs[String]("gpl")
      val wifi = row.getAs[String]("wifi")
      val wifis = wifi.split("\\|")
      for (i <- wifis.indices) {
        arr.+=(gpl + "|" + wifis(i))
      }
      arr
    }).map(line => {
      val gpl = line.split("\\|")(0).split("-")
      val wifi = line.split("\\|")(1)
      Row(wifi, gpl(0), gpl(1))
    })

    val structType = StructType(Array(
      StructField("wifi", DataTypes.StringType, nullable = true),
      StructField("lon", DataTypes.StringType, nullable = true),
      StructField("lat", DataTypes.StringType, nullable = true)
    ))

    spark.sqlContext.createDataFrame(flatData, structType).createOrReplaceTempView("tmp_wifi")

    val dataset = spark.sql("select wifi, cast(avg(lon) as string) as lon, cast(avg(lat) as string) as lat from tmp_wifi group by wifi")
    println("========", dataset.count())
    dataset.collect().foreach(println(_))

    //    dataset.foreachPartition(rows => {
    //      val listPut = new util.ArrayList[Put]()
    //      rows.foreach(row => {
    //        val ci = row(0).toString
    //        val lac = row(1).toString
    //        //row_key
    //        val bsRowKey = ci + HBASE_ROWKEY_SPLIT + lac
    //        //数据不多，但默认20个分区，因为我们有20台机器
    //        val put: Put = new Put(Bytes.toBytes(RowKeyUtils.getGPSRowKey(bsRowKey, HBASE_GPS_BS_REGION_NUM)))
    //        if (null != row(2) && null != row(3)) {
    //          put.addColumn(Bytes.toBytes(HBASE_FAMILY_NAME), Bytes.toBytes("b_lon"), Bytes.toBytes(row.getString(2)))
    //          put.addColumn(Bytes.toBytes(HBASE_FAMILY_NAME), Bytes.toBytes("b_lat"), Bytes.toBytes(row.getString(3)))
    //          listPut.add(put)
    //        }
    //        if (listPut.size() >= 5000) {
    //          HBASE_TABLE_BS_GPS.put(listPut)
    //          listPut.clear()
    //        }
    //      })
    //      if (listPut.size() > 0) {
    //        HBASE_TABLE_BS_GPS.put(listPut)
    //      }
    //    })

    //      tom = DateUtils.getNextMonth(tom)
    //    }

    spark.stop()
  }

  /**
    * 根据WiFi名称过滤掉掉公众WiFi
    *
    * @param str WiFi名称
    * @return
    */
  def filterPublicWiFi(str: String = "CHINA"): Boolean = {
    val blacklist = Array[String]("CHINA", "CMCC", "CHINANET", "中国")
    val context =
      try {
        str.toUpperCase
      }
      catch {
        case e: Exception =>
          "CHINA"
      }
    var is = false
    for (i <- blacklist.indices) {
      if (context.indexOf(blacklist(i)) != -1 && !is) {
        is = true
      }
    }
    is
  }
}
