package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.hive.{PutHiveStreaming, SelectHiveQL}
import cn.piflow.{FlowImpl, Path, Runner, Stop}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class ClassFindTest {

  @Test
  def testClassFind(): Unit = {
    val selectHiveQLClassName = "cn.cnic.bigdata.bundle.hive.SelectHiveQL"
    val putHiveStreamingClassName = "cn.cnic.bigdata.bundle.hive.PutHiveStreaming"
    //classOf[SelectHiveQL].getConstructor(classOf[Map[String, String]]).newInstance(map)

    val selectHiveQLParameters : Map[String, String] = Map("hiveQL" -> "select * from sparktest.student")
    val putHiveStreamingParameters : Map[String, String] = Map("database" -> "sparktest", "table" -> "studenthivestreaming")

    val selectHiveQLStop = Class.forName(selectHiveQLClassName).getConstructor(classOf[Map[String, String]]).newInstance(selectHiveQLParameters)
    val putHiveStreamingStop = Class.forName(putHiveStreamingClassName).getConstructor(classOf[Map[String, String]]).newInstance(putHiveStreamingParameters)

    val flow = new FlowImpl();
    flow.addStop("SelectHiveQL", selectHiveQLStop.asInstanceOf[Stop]);
    flow.addStop("PutHiveStreaming", putHiveStreamingStop.asInstanceOf[Stop]);
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
  }

}
