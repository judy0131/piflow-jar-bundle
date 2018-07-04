package cn.cnic.bigdata.bundle.common

import cn.cnic.bigdata.util.OptionUtil
import cn.piflow._
import org.apache.spark.sql.{Column, DataFrame}


class SelectField(map : Map[String, String]) extends Stop {

  val schema:String = OptionUtil.get(map.get("schema"))

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val df = in.read()

    val field = schema.split(",")
    val columnArray : Array[Column] = new Array[Column](field.size)
    for(i <- 0 to field.size - 1){
      columnArray(i) = new Column(field(i))
    }


    var finalFieldDF : DataFrame = df.select(columnArray:_*)
    finalFieldDF.show(2)

    out.write(finalFieldDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}



