/*
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.spark._
import com.couchbase.spark.streaming._
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

import com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;
import com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import com.couchbase.spark.japi.CouchbaseRDD.couchbaseRDD;
import com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;

/** A sample Apache Spark program to show how Couchbase may be used with Spark
  * when doing data transformations.
  *
  * Assuming a MySQL Database and documents with this format:
  *
  * {
  *  "givenname": "Matt",
  *   "surname": "Ingenthron",
  *   "email": "matt@email.com"
  * }
  *
  * Stream out all documents, look them up in the data loaded from mysql, join on
  * the email address and add the entitlement token.
  */
object ReadFromMySQL {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("ReadFromMySQLExample")
    .set("com.couchbase.bucket.temp", "") // Configure for the Couchbase bucket "transformative" with "password"

  val sc = new SparkContext(conf)

  /** Returns a JsonDocument based on a tuple of two strings */
  def CreateDocument(s: (String, String)): JsonDocument = {
    JsonDocument.create(s._1, JsonObject.fromJson(s._2))
  }

  /** Returns an RDD based on email address extraced from the document */
  def CreateMappableRdd(s: (String, String)): (String, JsonDocument) = {
    val return_doc = JsonDocument.create(s._1, JsonObject.fromJson(s._2))
    (return_doc.content().getString("email"), return_doc)
  }

  /** Returns a JsonDocument enriched with the entitlement token */
  def mergeIntoDoc(t: (String, (JsonDocument, Integer))): JsonDocument = {
    val jsonToEnrich = t._2._1.content()
    val entitlementFromJoin = t._2._2
    jsonToEnrich.put("entitlementtoken", entitlementFromJoin)
    t._2._1
  }

  def getMysqlReader(sqlctx: SQLContext): DataFrameReader = {

    // Now get set up to fetch things from MySQL
    // The database name is ext_users with the data we want in a table named profiles
    // and a read-only user named profiles
    val mysql_connstr = "jdbc:mysql://localhost:3306/ext_users"
    val mysql_uname = "profiles"
    val mysql_password = "profiles"

    sqlctx.read.format("jdbc").options(
      Map("url" -> (mysql_connstr + "?user=" + mysql_uname + "&password=" + mysql_password),
        "dbtable" -> "ext_users.profiles"))
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.dcpEnabled", "true")

    Class.forName("com.mysql.jdbc.Driver").newInstance // Load the MySQL Connector

    val mysqlReader = getMysqlReader(new org.apache.spark.sql.SQLContext(sc)) // set up a MySQL Reader

    // Note that if the database was quite large you could push down other predicates to MySQL or partition
    // the DataFrame
    //    mysqlReader.load().filter("email = \"matt@email.com\"")

    // load the DataFrame of all of the users from MySQL.
    // Note, appending .cache() may make sense here (or not) depending on amount of data.
    val entitlements_dataFrame = mysqlReader.load()



    /* loading this:
      +---------+-----------+-----------------+----------------+
      |givenname|    surname|            email|entitlementtoken|
      +---------+-----------+-----------------+----------------+
      |     Matt| Ingenthron|   matt@email.com|           11211|
      |  Michael|Nitschinger|michael@email.com|           11210|
      +---------+-----------+-----------------+----------------+
     */

    entitlements_dataFrame.show()

    entitlements_dataFrame.registerTempTable("names")

    entitlements_dataFrame.sqlContext.sql("select * from names").collect.foreach(println)

    //Test to write docs
    val doc1 = JsonDocument.create("doc1", JsonObject.create().put("some", "content"))
    val doc2 = JsonArrayDocument.create("doc2", JsonArray.from("more", "content", "in", "here"))
    val data = sc
      .parallelize(Seq(doc1, doc2))
      .saveToCouchbase()

    val rows = entitlements_dataFrame.collectAsList()
    val row = rows.get(1);
    println(row)

    val rowsList = entitlements_dataFrame.sqlContext.sql("select * from names").collect
    rowsList.map( row => println("Map " + row))
    rowsList.map( row => {
      val doc = JsonDocument.create(row.fieldIndex("email").toString,
        JsonObject.empty()
          .put("givenname", row.getAs("givenname").toString)
          .put("surname", row.getAs("surname").toString)
          .put("email", row.getAs("email").toString)
          .put("entitlementtoken", row.getAs("entitlementtoken").asInstanceOf[Int]))

      val data = sc
        .parallelize(Seq(doc))
        .saveToCouchbase()
      }

    )



//    entitlements_dataFrame.sqlContext.sql("select * from names").collect.map(
//      row =>
//        sc
//          .couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
//          .map(oldDoc => {
//            val id = "my_" + oldDoc.id()
//            val content = JsonObject.create().put("name", oldDoc.content().getString("name"))
//            JsonDocument.create(id, content)
//          })
//          .saveToCouchbase()
//    )

//                  parallelize(Arrays.asList(JsonDocument.create("doc1", JsonObject.empty().
//                  put("name","terry").put("married", true).put("name","terry4"))))
//              ).saveToCouchbase()
//    )


    //entitlements_dataFrame.sqlContext.sql("select * from names").collect.foreach(
    //      couchbaseDocumentRDD(
//        sc.parallelize(Arrays.asList(JsonDocument.create("doc1", JsonObject.empty().
//          put("name","terry").put("married", true).put("name","terry4"))))
//      ).saveToCouchbase())

    val entitlementsSansSchema = entitlements_dataFrame.rdd.map[(String, Integer)](f => (f.getAs[String]("email"), f.getAs[Integer]("entitlementtoken")))

    val ssc = new StreamingContext(sc, Seconds(5))


//
//    ssc.couchbaseStream("transformative")
//      .filter(_.isInstanceOf[Mutation])
//      .map(m => (new String(m.asInstanceOf[Mutation].key), new String(m.asInstanceOf[Mutation].content)))
//      .map(s => CreateMappableRdd(s))
//      .filter(_._2.content().get("entitlementtoken").eq(null))
//      .foreachRDD(rdd => {
//        rdd
//          .join(entitlementsSansSchema)
//          .map(mergeIntoDoc)
//          //.foreach(println) // a good place to see the effect
//          .saveToCouchbase("transformative")
//      })
//
//    ssc.start()
//    ssc.awaitTermination()
  }

}


