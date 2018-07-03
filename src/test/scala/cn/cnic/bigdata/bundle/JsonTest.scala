package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.jdbc.{JDBCRead, JDBCWrite}
import cn.cnic.bigdata.bundle.json.{JsonPathParser, JsonSave}
import cn.piflow.{FlowImpl, Path, Runner}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class JsonTest {

  @Test
  def testJsonPathParser(): Unit ={

    val jsonPath = "hdfs://10.0.86.89:9000/xjzhu/student.json"
    val tag = "student"
    val jsonSavePath = "hdfs://10.0.86.89:9000/xjzhu/example_json_save"

    val flow = new FlowImpl();

    flow.addStop("JsonPathParser", new JsonPathParser(jsonPath, tag));
    flow.addStop("JsonSave", new JsonSave(jsonSavePath));
    flow.addPath(Path.from("JsonPathParser").to("JsonSave"));

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


  @Test
  def testJsonStringParser(): Unit ={

  }

}
