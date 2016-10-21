package sparkFundamentals

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.{SparkConf, SparkContext}

/*
  sbt package
  spark-submit --class "Evaluator" --master "local[*]" target/scala-2.10/couchbase-spark-samples_2.10-1.0.0-SNAPSHOT.jar
 */

object Evaluator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Language Evaluator")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class",
      "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page>")
    jobConf.set("stream.recordreader.end", "</page>")
    FileInputFormat.addInputPaths(jobConf, "file:////Users/terry/dev/sandbox/apache-spark-fundamentals/2-apache-spark/Data/WikiPages_BigData.xml")

    val wikiDocuments = sc.hadoopRDD(jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text], classOf[Text])

    println("wikiDocuments count = " + wikiDocuments.count())
  }
}
