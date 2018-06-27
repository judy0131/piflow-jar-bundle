package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.xml.{XmlParser, XmlSave}
import cn.cnic.bigdata.hive.{PutHiveStreaming, SelectHiveQL}
import cn.piflow._
import org.apache.spark.sql.SparkSession
import org.junit.Test


class HiveTest {

  val hdfsURI = "hdfs://10.0.86.89:9000"
  val dataframeHdfsPath = hdfsURI + "/xjzhu/"

  @Test
  def testHive(): Unit = {

    val flow = new FlowImpl();

    flow.addProcess("SelectHiveQL", new SelectHiveQL("select * from sparktest.student"));
    flow.addProcess("PutHiveStreaming", new PutHiveStreaming("sparktest","studenthivestreaming"));
    flow.addPath(Path.of("SelectHiveQL"->"PutHiveStreaming"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("piflow-hive-bundle")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow-jar-bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val exe = Runner.create()
      .bind(classOf[SparkSession].getName, spark)
      .schedule(flow);

    exe.start();


  }

}
