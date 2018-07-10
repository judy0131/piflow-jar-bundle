package cn.cnic.bigdata.bundle.xml

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.cnic.bigdata.configure.bean.PropertyDescriptor
import cn.cnic.bigdata.util.MapUtil
import cn.piflow._

class XmlSave extends ConfigurableStop{

  var xmlSavePath:String = _

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val xmlDF = in.read()
    xmlDF.show()

    xmlDF.write.format("xml").save(xmlSavePath)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  override def setProperties(map: Map[String, Any]): Unit = {
    xmlSavePath = MapUtil.get(map,"xmlSavePath").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = ???
}
