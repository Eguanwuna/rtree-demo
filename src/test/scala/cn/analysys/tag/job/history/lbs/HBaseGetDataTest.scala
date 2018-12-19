package cn.analysys.tag.job.history.lbs

object HBaseGetDataTest {

//  val FORMAT_DATA = "yyyyMMdd"
//  private val conf = HBaseConfiguration.create
//  conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk14")
//  conf.set("hbase.zookeeper.property.clientPort", "2181")
//  conf.set("zookeeper.znode.parent", "/hbase")
//  conf.set("zookeeper.recovery.retry", "10")
//  val HBASE_TABLE_BS_GPS = new HTable(conf, "dim_tag_base_station_gps")
//  val HBASE_FAMILY_NAME = "i"
//  val HBASE_ROWKEY_SPLIT = ":"
//  val HBASE_GPS_WIFI_REGION_NUM: Int = 100
//  val HBASE_GPS_BS_REGION_NUM: Int = 20

  def main(args: Array[String]): Unit = {
//    val get = new Get(Bytes.toBytes("00-18813698:4257"))
//    val get = new Get(Bytes.toBytes("00-100004355:25856"))
//    val get = new Get(Bytes.toBytes("00-10002:9266"))
//    println("start get ......")
//    val rs = HBASE_TABLE_BS_GPS.get(get)
//    println("end get......")
//    rs.rawCells.foreach(cell =>
//      println(Bytes.toString(CellUtil.cloneQualifier(cell)) + "==" + Bytes.toString(CellUtil.cloneValue(cell)))
//    )
    val a:Double = 106.33933326936865
    val b:Double = 28.19594912017574

    val at:String = "106.33933326936865"
    val bt:String = "28.19594912017574"

    val rea = Bytes.toBytes(a)
    val reat = Bytes.toBytes(at)
    val reb = Bytes.toBytes(b)
    val rebt = Bytes.toBytes(bt)

    println(new String(reat, "utf8"))
    println(new String(reat))

    println(java.lang.Double.parseDouble(new String(rea)))


  }
}
