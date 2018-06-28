package cn.cnic.bigdata.bundle.json

import cn.piflow._
import org.apache.spark.sql.SparkSession

class JsonPathParser(jsonPath: String) extends Stop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val spark = pec.get[SparkSession]()

    val jsonDF = spark.read.option("multiline","true")json(jsonPath)
    jsonDF.show(10)
    out.write(jsonDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}

class JsonStringParser(jsonString: String) extends Stop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val spark = pec.get[SparkSession]()
    val jsonRDD = spark.sparkContext.makeRDD(jsonString :: Nil)
    val jsonDF = spark.read.json(jsonRDD)

    jsonDF.show(10)
    out.write(jsonDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}
