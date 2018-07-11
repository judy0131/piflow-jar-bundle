package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.configure.bean.FlowBean
import cn.cnic.bigdata.util.{FileUtil, OptionUtil}
import cn.piflow.Runner
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.util.parsing.json.JSON

class FlowTest {

  @Test
  def testFlow(): Unit ={

    //parse flow json
    val file = "src/main/resources/flow.json"
    val flowJsonStr = FileUtil.fileReader(file)
    val map = OptionUtil.getAny(JSON.parseFull(flowJsonStr)).asInstanceOf[Map[String, Any]]
    println(map)

    //create flow
    val flowBean = FlowBean(map)
    val flow = flowBean.constructFlow()

    //execute flow
    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("piflow-hive-bundle")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-jar-bundle/out/artifacts/piflow_jar_bundle/piflow-jar-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val process = Runner.create()
      .bind(classOf[SparkSession].getName, spark)
      .start(flow);

    process.awaitTermination();
    spark.close();
  }

}
