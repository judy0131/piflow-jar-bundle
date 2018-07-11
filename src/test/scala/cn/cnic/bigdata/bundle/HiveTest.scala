package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.xml.{XmlParser, XmlSave}
import cn.cnic.bigdata.bundle.hive.{PutHiveStreaming, SelectHiveQL}
import cn.piflow._
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.io.Source


class HiveTest {

  val selectHiveQLParameters : Map[String, String] = Map("hiveQL" -> "select * from sparktest.student")
  val putHiveStreamingParameters : Map[String, String] = Map("database" -> "sparktest", "table" -> "studenthivestreaming")

  /*@Test
  def testHive(): Unit = {

    val selectHiveQLStop = new SelectHiveQL
    selectHiveQLStop.setProperties(selectHiveQLParameters)

    val putHiveStreamingStop = new PutHiveStreaming
    putHiveStreamingStop.setProperties(putHiveStreamingParameters)

    val flow = new FlowImpl();

    flow.addStop("SelectHiveQL", selectHiveQLStop);
    flow.addStop("PutHiveStreaming", putHiveStreamingStop);
    flow.addPath(Path.from("SelectHiveQL").to("PutHiveStreaming"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("piflow-hive-bundle")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow-jar-bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val process = Runner.create()
      .bind(classOf[SparkSession].getName, spark)
      .start(flow);

    process.awaitTermination();
    spark.close();

  }*/

  @Test
  def testHiveQl() = {

    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("piflow-hive-bundle")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow-jar-bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    //val hiveql = spark.read.textFile("hdfs://10.0.86.89:9000/xjzhu/test.hiveql")
    val hiveql = Source.fromFile("/opt/data/test.hiveql")

    hiveql.getLines().foreach( x =>{
      println(x)
      sql(x)
    }
    )
  }

}
