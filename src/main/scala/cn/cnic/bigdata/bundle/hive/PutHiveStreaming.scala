package cn.cnic.bigdata.hive

import cn.cnic.bigdata.util.OptionUtil
import cn.piflow._
import org.apache.spark.sql.SparkSession
/*class PutHiveStreaming( database:String, table:String) extends Stop {


  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val inDF = in.read()
    inDF.show()

    val dfTempTable = table + "_temp"
    inDF.createOrReplaceTempView(dfTempTable)
    spark.sql("insert into " + database + "." + table +  " select * from " + dfTempTable)
    //out.write(studentDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}*/
class PutHiveStreaming(map : Map[String, String]) extends Stop {

  val database:String = OptionUtil.get(map.get("database"))
  val table:String = OptionUtil.get(map.get("table"))

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()
    val inDF = in.read()
    inDF.show()

    val dfTempTable = table + "_temp"
    inDF.createOrReplaceTempView(dfTempTable)
    spark.sql("insert into " + database + "." + table +  " select * from " + dfTempTable)
    //out.write(studentDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}
