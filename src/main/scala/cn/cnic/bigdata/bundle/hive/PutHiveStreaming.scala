package cn.cnic.bigdata.hive

import cn.piflow._
import org.apache.spark.sql.SparkSession
class PutHiveStreaming( database:String, table:String) extends Process{


  def perform(in: ProcessInputStream, out: ProcessOutputStream, pec: ProcessExecutionContext): Unit = {
    val spark = pec.get[SparkSession]()
    val inDF = in.read()
    inDF.show()

    val dfTempTable = table + "_temp"
    inDF.createOrReplaceTempView(dfTempTable)
    spark.sql("insert into " + database + "." + table +  " select * from " + dfTempTable)
    //out.write(studentDF)
  }

  def initialize(ctx: FlowExecutionContext): Unit = {

  }
}
