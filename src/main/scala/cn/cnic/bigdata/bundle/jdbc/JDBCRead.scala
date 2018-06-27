package cn.cnic.bigdata.bundle.jdbc

import cn.piflow._
import org.apache.spark.sql.SparkSession

class JDBCRead(driver:String, url:String, user:String, password:String, sql:String) extends Stop  {

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val dbtable = "( "  + sql + ") AS Temp"
    val jdbcDF = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password",password)
      .load()
    jdbcDF.show(10)
    out.write(jdbcDF)

  }

  def initialize(ctx: ProcessContext): Unit = {

  }

}
