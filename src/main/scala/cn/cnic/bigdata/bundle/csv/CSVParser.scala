package cn.cnic.bigdata.bundle.csv

import cn.piflow._
import org.apache.spark.sql.SparkSession


class CSVParser(csvPath: String, header: Boolean, delimiter: String) extends Stop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val spark = pec.get[SparkSession]()

    val csvDF = spark.read
      .option("header",header)
      .option("inferSchema","true")
      .option("delimiter",delimiter)
      /*.schema(schema)*/
      .csv(csvPath)
    csvDF.show(10)
    out.write(csvDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}

