package cn.analysys.tag.job.history.lbs

import java.awt.geom.Point2D
import java.io.Serializable

import com.github.davidmoten.grumpy.core.Position
import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.{Geometries, Geometry, Point, Rectangle}

object LaoNingLbsTest {

  /**
    * 33：香港， 34：澳门，35：台湾
    */
  val SPECIAL_AREA = List(33, 34, 35)

  @SerialVersionUID(-4353213125546424324L)
  case class FetchDev(id: Int, device_id: String, lat: Double, lon: Double, day: String) extends Serializable

  //根据面积求半径
  def getRedius(s: Double): Double = {
    Math.sqrt(s / Math.PI) / 1000 + 0.02
  }

  def createRediusBounds(from: Position, distanceKm: Double): Rectangle = {
    // performance you require you wouldn't have to be this accurate because
    // accuracy is enforced later
    val north = from.predict(distanceKm, 0)
    val south = from.predict(distanceKm, 180)
    val east = from.predict(distanceKm, 90)
    val west = from.predict(distanceKm, 270)
    Geometries.rectangle(west.getLon, south.getLat, east.getLon, north.getLat)
  }

  //求mbr
  def createRectangleBounds(from: scala.collection.immutable.List[Point]): Rectangle = {
    val lons = from.map(_.x().toDouble)
    val lats = from.map(_.y().toDouble)
    val maxLon: Double = lons.max
    val minLon: Double = lons.min
    val maxLat: Double = lats.max
    val minLat: Double = lats.min
    Geometries.rectangle(minLon, minLat, maxLon, maxLat)
  }

  //判断为空, 空或'null'则为true, 不为空为false
  def nullOrEmpty(str: String): Boolean = {
    if (StringUtils.equalsIgnoreCase(str, "null") || Strings.isNullOrEmpty(str)) {
      return true
    }
    false
  }

  //返回小数的小数点后的位数
  def decimalScale(data: Double): Int = {
    import java.math.BigDecimal
    val bd: BigDecimal = new BigDecimal(String.valueOf(data))
    bd.scale()
  }

  //判断点是否在围栏中所用
  def getGeoPointList(shape: String): List[Point] = {
    getPointList(shape).map(pt => Geometries.pointGeographic(pt._1, pt._2)).toList
  }

  //根据围栏，组成可Point集合
  private def getPointList(shape: String): ListBuffer[(Double, Double)] = {
    val lonlats = shape.split(";")
    val pts = ListBuffer[(Double, Double)]()
    for (lonlat <- lonlats) {
      val point = lonlat.split(",")
      val lon_d = point(0).toDouble
      val lat_d = point(1).toDouble
      val gcj2wgs_lat_lon = GpsUtils.gcj02towgs84(lat_d, lon_d)
      pts.+=((gcj2wgs_lat_lon._2, gcj2wgs_lat_lon._1))
    }
    pts
  }

  //给空间索引搜索用的
  def getInPointList(shape: String): List[Point2D.Double] = {
    getPointList(shape).map(pt => new Point2D.Double(pt._1, pt._2)).toList
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.exit(0)
    }

    val day = args(0)

    // 创建 spark 对象
    val spark = SparkSession.builder().appName("LaoNingLbsTest")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.app.id", "LaoNingLbsTest")
      .master("local[1]")
      .getOrCreate()

    //加载围栏码表
    val url = "jdbc:mysql://192.168.220.200:3306/ecdc?useUnicode=true&characterEncoding=UTF-8"
    val table = "dim_base_city_shape"
    //读MySQL的方法1
    val reader = spark.read.format("jdbc")
    reader.option("url", url)
    reader.option("dbtable", table)
    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("user", "dim_ecdc")
    reader.option("password", "d7xvqw9c8s")
    val sourceShapeDF = reader.load().persist(StorageLevel.MEMORY_ONLY_SER_2)

    sourceShapeDF.printSchema()
    //标准化围栏码表
    val rectangles = sourceShapeDF
      .collect()
      .filter(row => {
        var flag = true
        val id = row.getAs[Int]("id")
        val shape = row.getAs[String]("shape")
        val level = row.getAs[Int]("level")
        if (level != 2) {
          flag = false
        }
        if (nullOrEmpty(shape)) {
          flag = false
        }
        if (SPECIAL_AREA.contains(id)){
          flag = true
        }
        flag
      })
      .flatMap(row => {
        val id = row.getAs[Int]("id")
        val sort = row.getAs[Int]("sort") //加工次序，值越大，越靠后
        val name = row.getAs[String]("name")
        var shape = row.getAs[String]("shape")
        val listShape = ListBuffer[(Int, String, String, Rectangle, Int)]()
        if (shape.contains("-")){
          val shapes = shape.split("-")
          for (i <- shapes.indices) {
            shape = shapes(i)
            val points = getGeoPointList(shape)
            val rectangle = createRectangleBounds(points)
            listShape.+=((id, name, shape, rectangle, sort))
          }
        }else{
          val points = getGeoPointList(shape)
          val rectangle = createRectangleBounds(points)
          listShape.+=((id, name, shape, rectangle, sort))
        }
        listShape
      }).filter(_._1 != 0)

    println("===================rectangles.length", rectangles.length)

    //广播围栏数据
    val broadRectangleList = spark.sparkContext.broadcast(rectangles)
    //处理源数据
    val data = spark.sparkContext.textFile("C:\\Code\\Yi-Git\\analysysTAG\\src\\test\\resources\\laoninglbstest.txt")
      .map(line => {
        val fileds = line.split(",")
        Row(fileds(0), fileds(1), fileds(2), fileds(3), fileds(4), fileds(5), fileds(6), fileds(7))
      })

    val sourceType = StructType(Array(
      StructField("device_id", DataTypes.StringType, nullable = true),
      StructField("gpl", DataTypes.StringType, nullable = true),
      StructField("wifi_mac_address", DataTypes.StringType, nullable = true),
      StructField("wifi_level", DataTypes.StringType, nullable = true),
      StructField("location_area_code", DataTypes.StringType, nullable = true),
      StructField("cell_id", DataTypes.StringType, nullable = true),
      StructField("mobile_corporation", DataTypes.StringType, nullable = true),
      StructField("collection_time", DataTypes.StringType, nullable = true)
    ))

    spark.sqlContext.createDataFrame(data, sourceType)
      .toDF("device_id", "gpl", "wifi_mac_address", "wifi_level", "location_area_code",
        "cell_id", "mobile_corporation", "collection_time")
      .filter(row => {
        var flag = true
        val gpl = row.getAs[String]("gpl")
        val wifi_mac_address = row.getAs[String]("wifi_mac_address")
        val location_area_code = row.getAs[String]("location_area_code")
        val cell_id = row.getAs[String]("cell_id")
        val mobile_corporation = row.getAs[String]("mobile_corporation")
        // GPS坐标精度：X和Y坐标至少保留5位小数，如果不满足此条件认为当前记录无效，不计入总量统计。
        gpl.split("-").filter(!nullOrEmpty(_)).map(_.toDouble).foreach(d => {
          if (decimalScale(d) < 5) flag = false
        })
        //	如果MAC地址字段为空， Lac和 CellID字段则必须填写，同时运营商标识也必须填写，如果Lac和CellID也为空则记录不计数
        //	如果Lac和 CellID字段为空，MAC地址则为必填字段，否则，记录不计数
        // 其中OperatorsFlag字段为条件必填字段，在Lac和CellID字段不为空时为必填字段
        if (nullOrEmpty(wifi_mac_address)) {
          if (nullOrEmpty(cell_id) || nullOrEmpty(location_area_code)) {
            flag = false
          } else {
            if (nullOrEmpty(mobile_corporation)) {
              flag = false
            }
          }
        }
        flag
      })
      //      .rdd
      .map(row => {
      val userid = MD5.encrypt32(row.getAs[String]("device_id"))
      val gpl = row.getAs[String]("gpl")
      val wifi_mac_address = row.getAs[String]("wifi_mac_address")
      val wifi_level = row.getAs[String]("wifi_level")
      val location_area_code = row.getAs[String]("location_area_code")
      val cell_id = row.getAs[String]("cell_id")
      val mobile_corporation = row.getAs[String]("mobile_corporation")
      val collection_time = row.getAs[String]("collection_time")
      //GPS标准化,  纬度-经度
      val gpsBuffer = ListBuffer[Double]()
      var gps1: Double = 0D
      var gps2: Double = 0D
      if (gpl.indexOf("--") > 1) {
        gps1 = gpl.split("--")(0).toDouble
        gps2 = -gpl.split("--")(1).toDouble
      } else if (gpl.startsWith("-")) {
        gps1 = -gpl.split("-")(1).toDouble
        gps2 = gpl.split("-")(2).toDouble
      } else {
        gps1 = gpl.split("-")(0).toDouble
        gps2 = gpl.split("-")(1).toDouble
      }
      if (Math.abs(gps2) > 90) {
        gpsBuffer.+=(gps1)
        gpsBuffer.+=(gps2)
      } else {
        gpsBuffer.+=(gps2)
        gpsBuffer.+=(gps1)
      }
      //wifi标准化
      val macs = wifi_mac_address.split("-").filter(!nullOrEmpty(_))
      val levels = wifi_level.split("-").filter(!nullOrEmpty(_))
      val wifiMacBuffer = ListBuffer[String]()
      val wifiLevelBuffer = ListBuffer[String]()
      if (levels.length >= macs.length) {
        for (i <- levels.indices) {
          wifiMacBuffer.+=(macs(i))
          wifiLevelBuffer.+=(levels(i))
        }
      } else {
        for (i <- macs.indices) {
          wifiMacBuffer.+=(macs(i))
          wifiLevelBuffer.+=(levels(i))
        }
      }
      (userid, collection_time, gpsBuffer.mkString("|"),
        wifiMacBuffer.mkString("-"), wifiLevelBuffer.mkString("-"), location_area_code, cell_id, mobile_corporation)
    })
      .toDF("userid", "time", "gps", "mac", "rssi", "lac", "cellid", "operatorsflag")
      .repartition(1)
      .mapPartitions((iter: Iterator[Row]) => {
        var tree: RTree[String, Geometry] = RTree.star().create()
        while (iter.hasNext) {
          val row = iter.next
          val tag = ListBuffer[String]()
          val userid = row.getAs[String]("userid")
          val time = row.getAs[String]("time")
          val gps = row.getAs[String]("gps")
          val mac = row.getAs[String]("mac")
          val rssi = row.getAs[String]("rssi")
          val lac = row.getAs[String]("lac")
          val cellid = row.getAs[String]("cellid")
          val operatorsflag = row.getAs[String]("operatorsflag")
          tag.+=(userid).+=(time).+=(gps).+=(mac).+=(rssi).+=(lac).+=(cellid).+=(operatorsflag)
          val base64 = BASE64.encryptBASE64(tag.mkString(","))
          val lat = gps.split("\\|")(0).toDouble
          val lon = gps.split("\\|")(1).toDouble
          val point: Point = Geometries.pointGeographic(lon, lat)
          //建空间索引
          tree = tree.add(base64, point)
        }
        val fetchDevs = ListBuffer[(String, String, String, String, String, String, String, String, String, Int)]()
        broadRectangleList.value.sortBy(_._5).foreach(rect => {
          val name = rect._2
          val shape = rect._3
          val rectangle = rect._4
          //粗粒度-判断点是否在围栏（MBR）内-节省计算时间
          val ret = tree.search(rectangle).toList.toBlocking.single().iterator()
          while (ret.hasNext) {
            val reD = ret.next()
            val decrypt = BASE64.decryptBASE64(reD.value())
            val tag = decrypt.split(",")
            val lat = tag(2).split("\\|")(0).toDouble
            val lon = tag(2).split("\\|")(1).toDouble
            //细粒度判断是否在围栏内
            val pts = getInPointList(shape)
            val bolEx = GpsUtils.isPtInPoly(new Point2D.Double(lon, lat), pts)
            if (bolEx) {
              fetchDevs.+=((tag(0), tag(1), tag(2), tag(3), tag(4), tag(5), tag(6), tag(7), name, 1))
              tree = tree.delete(reD.value(), reD.geometry())
            } else {
              fetchDevs.+=((tag(0), tag(1), tag(2), tag(3), tag(4), tag(5), tag(6), tag(7), name, 0))
            }
          }
        })

        val otherEntries = tree.entries().toList.toBlocking.single().iterator()
        while (otherEntries.hasNext) {
          val reD = otherEntries.next()
          val decrypt = BASE64.decryptBASE64(reD.value())
          val tag = decrypt.split(",")
          fetchDevs.+=((tag(0), tag(1), tag(2), tag(3), tag(4), tag(5), tag(6), tag(7), "非大陆", 1))
        }

        fetchDevs.iterator
      })
      .toDF("userid", "time", "gps", "mac", "rssi", "lac", "cellid", "operatorsflag", "city", "flag")
      .createOrReplaceTempView("tmp_fetch_dev_data")

    spark.sql(s"select userid, time, gps, mac, rssi, lac, cellid, operatorsflag, city, flag, " +
      s"'$day' as day, substring(time, 12, 2) as hour from tmp_fetch_dev_data distribute by day, hour sort by time asc")
      .createOrReplaceTempView("job_ln_mobile_gps")

    spark.sql(s"select * from job_ln_mobile_gps where day = '$day' and flag = 1")
      .show(10000, false)



    //    spark.conf.set("mapred.reduce.tasks", 5)
    //    spark.sql(s"select userid from job_ln_mobile_gps where day = '$day'")
    //      .write
    //      .mode(SaveMode.Overwrite)
    //      .format("com.databricks.spark.csv")
    ////      .option("header", "true")
    //      .csv(s"file:///data1_2T/data4tag/limin/lnmobile/$day")

    sourceShapeDF.unpersist
    broadRectangleList.unpersist()

    spark.stop()

  }
}