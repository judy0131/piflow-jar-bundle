package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.csv.CSVParser
import cn.cnic.bigdata.bundle.jdbc.{JDBCRead, JDBCWrite}
import cn.cnic.bigdata.bundle.json.JsonSave
import cn.piflow.{FlowImpl, Path, Runner}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class CSVTest {

  @Test
  def testCSVHeaderRead(): Unit ={

    val csvPath = "hdfs://10.0.86.89:9000/xjzhu/student.csv"
    val header = true
    val delimiter = ","
    val schema = ""
    val jsonPath = "hdfs://10.0.86.89:9000/xjzhu/student_csv2json"


    val flow = new FlowImpl();

    flow.addStop("CSVParser", new CSVParser(csvPath,header,delimiter, schema));
    flow.addStop("JsonSave", new JsonSave(jsonPath));
    flow.addPath(Path.from("CSVParser").to("JsonSave"));

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
  def testCSVSchemaRead(): Unit ={

    val csvPath = "hdfs://10.0.86.89:9000/xjzhu/student_schema.csv"
    val header = false
    val delimiter = ","
    val schema = "id,name,gender,age"
    val jsonPath = "hdfs://10.0.86.89:9000/xjzhu/student_schema_csv2json"


    val flow = new FlowImpl();

    flow.addStop("CSVParser", new CSVParser(csvPath,header,delimiter,schema));
    flow.addStop("JsonSave", new JsonSave(jsonPath));
    flow.addPath(Path.from("CSVParser").to("JsonSave"));

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
