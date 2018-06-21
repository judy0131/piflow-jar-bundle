package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.common.SelectField
import cn.cnic.bigdata.bundle.xml.XmlParser
import cn.cnic.bigdata.hive.PutHiveStreaming
import cn.piflow.{DependencyTrigger, Flow, FlowImpl, Runner}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Test

class XmlTest {

  @Test
  def testNodeXML(): Unit = {

    val flow: Flow = new FlowImpl();
    val xmlpath = "hdfs://10.0.86.89:9000/xjzhu/dblp.mini.xml"
    val rowTag = "phdthesis"
    val schema = StructType(Array(
      StructField("_key", StringType, nullable = true),
      StructField("_mdate", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("year", StringType, nullable = true),
      StructField("school", StringType, nullable = true),
      StructField("ee", StringType, nullable = true),
      StructField("note", StringType, nullable = true)
    ))
    val selectedField : String = "title,author,pages"

    flow.addProcess("XmlParser", new XmlParser( xmlpath,rowTag,schema,"dblp_phdthesis_mini"));
    flow.addProcess("SelectField", new SelectField( "dblp_phdthesis_mini",selectedField,"dblp_phdthesis_selectfield"));
    flow.addProcess("PutHiveStreaming", new PutHiveStreaming("dblp_phdthesis_selectfield","sparktest","dblp_phdthesis"));
    flow.addTrigger("SelectField", new DependencyTrigger("XmlParser"));
    flow.addTrigger("PutHiveStreaming", new DependencyTrigger("SelectField"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("DblpParserTest")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "3")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow-jar-bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val exe = Runner.bind("localBackupDir", "/tmp/")
      .bind(classOf[SparkSession].getName, spark)
      .run(flow);

    exe.start("XmlParser");
    Thread.sleep(30000);
    exe.stop();

  }

}
