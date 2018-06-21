package cn.cnic.bigdata.bundle.xml

import cn.piflow.Shadow
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import cn.piflow._
import cn.piflow.util.PropertyUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class XmlParser(xmlpath:String, rowTag:String, schema: StructType = null, dataframeOut:String) extends Process {



  override def shadow(pec: ProcessExecutionContext) = {

    val spark = pec.get[SparkSession]()

    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("treatEmptyValuesAsNulls",true)
      /*.schema(schema)*/
      .load(xmlpath)

    /*xmlDF.select("ee").rdd.collect().foreach( row =>
      println(row.toSeq)
    )*/
    xmlDF.show(30)


    new Shadow {
      override def discard(pec: ProcessExecutionContext): Unit = {
        //tmpfile.delete();
      }

      override def perform(pec: ProcessExecutionContext): Unit = {
        //TODO: delete the path
        val dataframeOutPath : String = PropertyUtil.getPropertyValue("dataframe_hdfs_path") + dataframeOut + ".parquet"
        val path = new Path(dataframeOutPath)
        val hdfs = FileSystem.get(new java.net.URI(PropertyUtil.getPropertyValue("hdfsURI")),spark.sparkContext.hadoopConfiguration)
        if (hdfs.exists(path)){
          System.out.println("Deleting " + dataframeOutPath + "!!!!!!!!!!!!!!")
          hdfs.delete(path,true)
        }
        xmlDF.write.parquet(dataframeOutPath)
      }

      override def commit(pec: ProcessExecutionContext): Unit = {
        //tmpfile.renameTo(new File("./out/wordcount"));
      }
    };

  }

  /**
    * Backup is used to perform undo()
    *
    * @param pec
    * @return
    */
  override def backup(pec: ProcessExecutionContext): Backup = ???
}


object XmlParser{

  def main(args: Array[String]): Unit = {
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

    flow.addProcess("XmlParser", new XmlParser( xmlpath,rowTag,schema,"dblp_phdthesis_mini"));
    //flow.addProcess("PutHiveStreaming", new PutHiveStreaming("student","sparktest","studenthivestreaming"));
    //flow.addTrigger("PutHiveStreaming", new DependencyTrigger("XmlParser"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("DblpParserTest")
      .config("spark.driver.memory", "8g")
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



