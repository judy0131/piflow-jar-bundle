package cn.cnic.bigdata.bundle.xml

import cn.piflow._

class XmlSave(xmlSavePath:String) extends Stop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val xmlDF = in.read()
    xmlDF.show()

    xmlDF.write.format("xml").save(xmlSavePath)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}
