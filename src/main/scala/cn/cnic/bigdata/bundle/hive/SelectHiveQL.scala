package cn.cnic.bigdata.bundle.hive

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.util.{MapUtil, OptionUtil}
import cn.piflow._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class SelectHiveQL extends ConfigurableStop {

  var hiveQL:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()

    import spark.sql
    val df = sql(hiveQL)
    df.show()

    out.write(df)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {
    hiveQL = MapUtil.get(map,"hiveQL").asInstanceOf[String]
  }
}


