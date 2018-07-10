package cn.cnic.bigdata.bundle.json

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.configure.bean.PropertyDescriptor
import cn.cnic.bigdata.util.MapUtil
import cn.piflow._
import org.apache.spark.sql.SaveMode

class JsonSave extends ConfigurableStop{

  var jsonSavePath: String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val jsonDF = in.read()
    jsonDF.show()

    jsonDF.write.format("json").mode(SaveMode.Overwrite).save(jsonSavePath)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  override def setProperties(map: Map[String, Any]): Unit = {
    jsonSavePath = MapUtil.get(map,"jsonSavePath").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = ???
}
