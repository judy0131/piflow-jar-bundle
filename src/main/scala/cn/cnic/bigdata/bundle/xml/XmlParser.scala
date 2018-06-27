package cn.cnic.bigdata.bundle.xml

import org.apache.hadoop.fs.{FileSystem}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import cn.piflow.{Path, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class XmlParser(xmlpath:String, rowTag:String, schema: StructType = null) extends Process {

  def perform(in: ProcessInputStream, out: ProcessOutputStream, pec: ProcessExecutionContext): Unit = {

    val spark = pec.get[SparkSession]()

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      /*.schema(schema)*/
      .load(xmlpath)

    /*xmlDF.select("ee").rdd.collect().foreach( row =>
      println(row.toSeq)
    )*/
    xmlDF.show(30)
    out.write(xmlDF)
  }

  def initialize(ctx: FlowExecutionContext): Unit = {

  }
}
