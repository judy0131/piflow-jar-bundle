package cn.cnic.bigdata.bundle.hive

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.util.{MapUtil, OptionUtil}
import cn.piflow._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/*
class SelectHiveQL(hiveQL:String) extends ConfigurableStop {


  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()

    import spark.sql
    val studentDF = sql(hiveQL)
    studentDF.show()

    out.write(studentDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

}*/

class SelectHiveQL extends ConfigurableStop {

  var hiveQL:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()

    import spark.sql
    val studentDF = sql(hiveQL)
    studentDF.show()

    out.write(studentDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {
    hiveQL = MapUtil.get(map,"hiveQL").asInstanceOf[String]
  }
}


