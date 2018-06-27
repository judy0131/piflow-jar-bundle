package cn.cnic.bigdata.bundle.common

import java.io.{File, FileInputStream, FileOutputStream}

import cn.cnic.bigdata.bundle.xml.{XmlParser, XmlSave}
import cn.piflow.{Flow, FlowExecutionContext, FlowImpl, Path, Process, ProcessExecutionContext, ProcessInputStream, ProcessOutputStream, Runner}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class SelectField(schema:String) extends Process {

  def perform(in: ProcessInputStream, out: ProcessOutputStream, pec: ProcessExecutionContext): Unit = {
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

  def initialize(ctx: FlowExecutionContext): Unit = {

  }
}



