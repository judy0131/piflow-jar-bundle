package cn.cnic.bigdata.bundle.jdbc

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.util.MapUtil
import cn.piflow._
import org.apache.spark.sql.SparkSession

class JDBCRead extends ConfigurableStop  {

  var driver:String = _
  var url:String = _
  var user:String = _
  var password:String = _
  var sql:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val dbtable = "( "  + sql + ") AS Temp"
    val jdbcDF = spark.read.format("jdbc")
      .option("url", url)
      //.option("driver", driver)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password",password)
      .load()
    jdbcDF.show(10)

    out.write(jdbcDF)

  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  override def setProperties(map: Map[String, Any]): Unit = {
    driver = MapUtil.get(map,"driver").asInstanceOf[String]
    url = MapUtil.get(map,"url").asInstanceOf[String]
    user = MapUtil.get(map,"user").asInstanceOf[String]
    password = MapUtil.get(map,"password").asInstanceOf[String]
    sql = MapUtil.get(map,"sql").asInstanceOf[String]
  }
}
