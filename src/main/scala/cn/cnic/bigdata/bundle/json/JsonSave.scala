package cn.cnic.bigdata.bundle.json

import cn.piflow._
import org.apache.spark.sql.SaveMode

class JsonSave( jsonSavePath: String) extends Stop{
  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val jsonDF = in.read()
    jsonDF.show()

    jsonDF.write.format("json").mode(SaveMode.Overwrite).save(jsonSavePath)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}
