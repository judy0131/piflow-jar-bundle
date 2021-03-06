name := "piflow-jar-bundle"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "junit" % "junit" % "4.11" % Test,
  "org.quartz-scheduler" % "quartz" % "2.3.0",
  "mysql" % "mysql-connector-java" % "5.1.40",
  "net.minidev" % "json-smart" % "2.3",
  "org.clapper" %% "classutil" % "1.3.0"

)