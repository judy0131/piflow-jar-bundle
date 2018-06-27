package cn.cnic.bigdata.bundle.xml

import cn.piflow.{FlowExecutionContext, Process, ProcessExecutionContext, ProcessInputStream, ProcessOutputStream}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class XmlSave(xmlSavePath:String) extends Process{

  def perform(in: ProcessInputStream, out: ProcessOutputStream, pec: ProcessExecutionContext): Unit = {
    val xmlDF = in.read()
    xmlDF.show()

    xmlDF.write.format("xml").save(xmlSavePath)
  }

  def initialize(ctx: FlowExecutionContext): Unit = {

  }
}
