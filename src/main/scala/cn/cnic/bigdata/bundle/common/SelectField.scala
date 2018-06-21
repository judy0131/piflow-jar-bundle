package cn.cnic.bigdata.bundle.common

import cn.piflow.{Backup, Flow, FlowImpl, Process, ProcessExecutionContext, Runner, Shadow}
import cn.piflow.util.PropertyUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class SelectField(dataframeIn:String, schema:String, dataframeOut:String) extends Process {



  override def shadow(pec: ProcessExecutionContext) = {

    val dataframeInPath = PropertyUtil.getPropertyValue("dataframe_hdfs_path") + dataframeIn + ".parquet"
    val spark = pec.get[SparkSession]()
    val df = spark.read.parquet(dataframeInPath)
    df.show(2)


    val field = schema.split(",")
    val columnArray : Array[Column] = new Array[Column](field.size)
    for(i <- 0 to field.size - 1){
      columnArray(i) = new Column(field(i))
    }


    var finalFieldDF : DataFrame = df.select(columnArray:_*)
    finalFieldDF.show(2)


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
        finalFieldDF.write.parquet(dataframeOutPath)
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

object SelectField{

  def main(args: Array[String]): Unit = {
    val flow: Flow = new FlowImpl();
    val schema :String = "title,author,pages"

    flow.addProcess("SelectField", new SelectField( "dblp_phdthesis_mini",schema,"dblp_phdthesis_selectfield"));
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

    exe.start("SelectField");
    Thread.sleep(30000);
    exe.stop();
  }
}


