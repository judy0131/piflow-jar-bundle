package cn.cnic.bigdata.bundle.csv

import cn.cnic.bigdata.util.OptionUtil
import cn.piflow._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class CSVParser(map : Map[String, String]) extends Stop{

  var csvPath: String = OptionUtil.get(map.get("csvPath"))
  val header: Boolean = OptionUtil.get(map.get("header")).toBoolean
  val delimiter: String = OptionUtil.get(map.get("delimiter"))
  val schema: String = OptionUtil.get(map.get("schema"))

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val spark = pec.get[SparkSession]()
    var csvDF:DataFrame = null
    if (header){
      csvDF = spark.read
        .option("header",header)
        .option("inferSchema","true")
        .option("delimiter",delimiter)
        /*.schema(schema)*/
        .csv(csvPath)


    }else{

      val field = schema.split(",")
      val structFieldArray : Array[StructField] = new Array[StructField](field.size)
      for(i <- 0 to field.size - 1){
        structFieldArray(i) = new StructField(field(i), StringType, nullable = true)
      }
      val schemaStructType = StructType(structFieldArray)

      csvDF = spark.read
        .option("header",header)
        .option("inferSchema","false")
        .option("delimiter",delimiter)
        .schema(schemaStructType)
        .csv(csvPath)
    }

    csvDF.show(10)
    out.write(csvDF)

  }

  def initialize(ctx: ProcessContext): Unit = {

  }
}

