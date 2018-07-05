package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.common.SelectField
import cn.cnic.bigdata.bundle.xml.XmlParser
import cn.cnic.bigdata.bundle.hive.PutHiveStreaming
import cn.piflow._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Test

class XmlTest {

  /*@Test
  def testNodeXML(): Unit = {

    val flow = new FlowImpl();

    /*val schema = StructType(Array(
      StructField("_key", StringType, nullable = true),
      StructField("_mdate", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("year", StringType, nullable = true),
      StructField("school", StringType, nullable = true),
      StructField("ee", StringType, nullable = true),
      StructField("note", StringType, nullable = true)
    ))*/
    val xmlParserMap = Map("xmlpath"->"hdfs://10.0.86.89:9000/xjzhu/dblp.mini.xml", "rowTag" -> "phdthesis")

    val selectedFieldMap = Map("selectedField"->"title,author,pages")

    val putHiveStreamingParametersMap = Map("database" -> "sparktest", "table" -> "dblp_phdthesis")

    flow.addStop("XmlParser", new XmlParser( xmlParserMap));
    flow.addStop("SelectField", new SelectField( selectedFieldMap));
    flow.addStop("PutHiveStreaming", new PutHiveStreaming(putHiveStreamingParametersMap));
    //flow.addPath(Path.from("XmlParser").to("SelectField").to("PutHiveStreaming"))
    flow.addPath(Path.from("XmlParser").to("SelectField"))
    flow.addPath(Path.from("SelectField").to("PutHiveStreaming"))


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("DblpParserTest")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "3")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow-jar-bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val process = Runner.create()
      .bind(classOf[SparkSession].getName, spark)
      .start(flow);

    process.awaitTermination();
    spark.close();
  }
*/
}
