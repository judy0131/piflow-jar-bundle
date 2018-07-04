package cn.cnic.bigdata.bundle.xml

import cn.cnic.bigdata.util.OptionUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import cn.piflow.{Path, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class XmlParser(map : Map[String, String]) extends Stop {

  val xmlpath:String = OptionUtil.get(map.get("xmlpath"))
  val rowTag:String = OptionUtil.get(map.get("rowTag"))
  val schema: StructType = null

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

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

  def initialize(ctx: ProcessContext): Unit = {

  }
}
