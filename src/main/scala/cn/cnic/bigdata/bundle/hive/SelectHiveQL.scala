package cn.cnic.bigdata.bundle.hive

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.configure.bean.PropertyDescriptor
import cn.cnic.bigdata.util.MapUtil
import cn.piflow._
import org.apache.spark.sql.SparkSession



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

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = null
    val hiveQL = new PropertyDescriptor().name("hiveQL").displayName("HiveQL").defaultValue("").required(true)
    descriptor = hiveQL :: descriptor
    descriptor
  }
}


