package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.bundle.jdbc.JDBCRead
import cn.cnic.bigdata.hive.{PutHiveStreaming, SelectHiveQL}
import cn.piflow.{FlowImpl, Path, Runner}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class JDBCTest {

  @Test
  def testMysqlRead(): Unit ={

    val url = "jdbc:mysql://10.0.86.90/sparktest"
    val driver = "com.mysql.jdbc.Driver"
    val sql = "select * from student"
    val user = "root"
    val password = "root"

    val flow = new FlowImpl();

    flow.addStop("JDBCRead", new JDBCRead(driver, url, user, password, sql));
    flow.addStop("PutHiveStreaming", new PutHiveStreaming("sparktest","studenthivestreaming"));
    flow.addPath(Path.from("JDBCRead").to("PutHiveStreaming"));

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
