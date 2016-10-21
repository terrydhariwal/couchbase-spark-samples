name := "couchbase-spark-samples"

organization := "com.couchbase"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"

assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala=false)

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "1.6.1",
//  "org.apache.spark" %% "spark-streaming" % "1.6.1",
//  "org.apache.spark" %% "spark-sql" % "1.6.1",
//  "com.couchbase.client" %% "spark-connector" % "1.2.1",
//  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
//  "org.apache.spark" %% "spark-mllib" % "1.6.1",
//  "mysql" % "mysql-connector-java" % "5.1.37",
//  "org.apache.hadoop" % "hadoop-streaming" % "2.6.0"
//)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "1.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.37" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.6.0" % "provided"
//libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0" % "provided"