name := "spark-wordcount"

version := "1.0"

scalaVersion := "2.10.4"

val SPARK_VERSION="1.6.1"
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % SPARK_VERSION

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % SPARK_VERSION
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % SPARK_VERSION
libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1"
