package sparkFundamentals

/**
  * Created by terry on 09/10/2016.
  */
import org.apache.spark.{SparkConf, SparkContext};

/* how to run on command line
  sbt package
  spark-submit --class "WordCounter" --master "local[*]" target/scala-2.10/couchbase-spark-samples_2.10-1.0.0-SNAPSHOT.jar
  cat /Users/terry/dev/sandbox/temp/ReadMeWordCountViaApp/part-00000
 */

object WordCounter {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("word counter")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/terry/dev/sandbox/spark-1.6.1-bin-hadoop2.6/README.md")
    val tokenizedFileData = textFile.flatMap(line => line.split(" "))
    val countPrep = tokenizedFileData.map(word => (word,1))
    val counts = countPrep.reduceByKey((accumVal, newVal)=> accumVal+newVal)
    val sortedCounts = counts.sortBy(kvPair => kvPair._2,false)
    sortedCounts.saveAsTextFile("/Users/terry/dev/sandbox/temp/ReadMeWordCountViaApp")

  }

  /*
  import org.apache.hadoop.io._
  val data = sc.parallelize(List(("key1", 1), ("Kay2", 2), ("Key3", 2)))
  data.saveAsSequenceFile("/tmp/seq-output")
  var result = sc.sequenceFile("file:///////tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile("file:////tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile("file:///tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile("file://tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile("file:/tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile("/tmp/seq-output", classOf[Text], classOf[IntWritable]). map{case (x, y) => (x.toString, y.get())}
  var result = sc.sequenceFile[String, Int]("file:/tmp/seq-output")
  var result = sc.sequenceFile[String, Int]("/Users/terry/dev/sandbox/spark-1.6.1-bin-hadoop2.6/README.md")
  var result = sc.sequenceFile[String, Int]("/Users/terry/dev/sandbox/temp/ReadMeWordCountViaApp/")
  result.collect
*/


}
