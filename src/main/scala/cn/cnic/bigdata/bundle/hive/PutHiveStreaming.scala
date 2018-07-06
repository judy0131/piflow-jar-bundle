package cn.cnic.bigdata.bundle.hive

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.util.{MapUtil, OptionUtil}
import cn.piflow._
import org.apache.spark.sql.SparkSession

class PutHiveStreaming extends ConfigurableStop {

  var database:String = _
  var table:String = _

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

  def setProperties(map : Map[String, Any]) = {
    database = MapUtil.get(map,"database").asInstanceOf[String]
    table = MapUtil.get(map,"table").asInstanceOf[String]
  }

}
