package cn.cnic.bigdata.hive

import cn.piflow._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class SelectHiveQL(hiveQL:String) extends Process {


  def perform(in: ProcessInputStream, out: ProcessOutputStream, pec: ProcessExecutionContext): Unit = {
    val spark = pec.get[SparkSession]()

    import spark.sql
    val studentDF = sql(hiveQL)
    studentDF.show()

    out.write(studentDF)
  }

  def initialize(ctx: FlowExecutionContext): Unit = {

  }

}


