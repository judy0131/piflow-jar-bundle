package cn.cnic.bigdata.hive

import cn.piflow._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class SelectHiveQL(hiveQL:String) extends Stop {


  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark = pec.get[SparkSession]()

    import spark.sql
    val studentDF = sql(hiveQL)
    studentDF.show()

    out.write(studentDF)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

}


