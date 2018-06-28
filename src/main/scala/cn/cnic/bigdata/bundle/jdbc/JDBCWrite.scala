package cn.cnic.bigdata.bundle.jdbc

import java.util.Properties

import cn.piflow._
import org.apache.spark.sql.{SaveMode, SparkSession}

class JDBCWrite(url:String, user:String, password:String, dbtable:String) extends Stop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val jdbcDF = in.read()
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    jdbcDF.write.mode(SaveMode.Append).jdbc(url,dbtable,properties)
    jdbcDF.show(10)
    out.write(jdbcDF)

  }

  def initialize(ctx: ProcessContext): Unit = {

  }

}
