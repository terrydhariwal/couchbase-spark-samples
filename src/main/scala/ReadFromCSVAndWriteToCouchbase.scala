import com.couchbase.spark._
import com.couchbase.client.java.document._
import com.couchbase.client.java.document.json.JsonObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object ReadFromCSVAndWriteToCouchbase {

  val conf = new SparkConf()
    .set("spark.driver.allowMultipleContexts", "true")
    .setMaster("local[*]")
    .setAppName("csvExample")
    .set("com.couchbase.bucket.temp", "")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/terry/dev/couchbase-ansible-4.x_/dataset.csv")

    df.show()

    df.registerTempTable("names")

    //df.sqlContext.sql("select * from names").collect.foreach(println)
    val rowsList = df.sqlContext.sql("select * from names").collect()
    val tableSchema = df.schema
    val rowsListRDD = sc.parallelize(rowsList)

    val docArray = rowsListRDD.map(row => {JsonDocument.create(row.getAs("email").toString, JsonObject.empty()
      .put("givenname", row.getAs("givenname").toString)
      .put("surname", row.getAs("surname").toString)
      .put("email", row.getAs("email").toString)
      .put("entitlementtoken", row.getAs("entitlementtoken").asInstanceOf[Int]))
    }).collect()

    val data = sc.parallelize(docArray)
      .saveToCouchbase()

  }

}



