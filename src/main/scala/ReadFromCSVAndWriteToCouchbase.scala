import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.couchbase.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrameReader, SQLContext};

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

   rowsList.foreach(row => {

     //println("FOREACH, writing row to Couchbase " + row + " on " + Thread.currentThread().getName)

     val doc = JsonDocument.create(row.getAs("email").toString,
       JsonObject.empty()
         .put("givenname", row.getAs("givenname").toString)
         .put("surname", row.getAs("surname").toString)
         .put("email", row.getAs("email").toString)
         .put("entitlementtoken", row.getAs("entitlementtoken").asInstanceOf[Int]))

     val data = sc
       //.parallelize(Seq(doc, doc2)) //this could be a list of documents to save as apposed to one!
       .parallelize(Seq(doc))
       .saveToCouchbase()
   })

  }

}


