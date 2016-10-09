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
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

object ReadFromMySQLAndWriteToCouchbase {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("ReadFromMySQLExample")
    .set("com.couchbase.bucket.temp", "") // Configure for the Couchbase bucket "transformative" with "password"

  val sc = new SparkContext(conf)

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

    rowsList.foreach(row => println("FOREACH, displaying " + row + " on " + Thread.currentThread().getName))

    rowsList.foreach(row => {

      println("FOREACH, writing row to Couchbase " + row + " on " + Thread.currentThread().getName)

      val doc = JsonDocument.create(row.fieldIndex("email").toString,
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

    //Map equivalent to foreach
/*    rowsList.map( row => println("MAP, displaying " + row + " on " + Thread.currentThread().getName))

    rowsList.map( row => {

      println("MAP, writing row to Couchbase " + row + " on " + Thread.currentThread().getName)

      val doc = JsonDocument.create(row.fieldIndex("email").toString,
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
*/

    //run as RDD - which will parallelize processing across threads :-D
    val rowsListRDD = sc.parallelize(rowsList)
    rowsListRDD.foreach(rowRDD => println("FOREACH, displaying " + rowRDD + " on " + Thread.currentThread().getName))


    rowsListRDD.foreach(rowRDD => {

      println("FOREACH, writing rowRDD to Couchbase " + rowRDD + " on " + Thread.currentThread().getName)

      val doc = JsonDocument.create(rowRDD.fieldIndex("email").toString,
        JsonObject.empty()
          .put("givenname", rowRDD.getAs("givenname").toString)
          .put("surname", rowRDD.getAs("surname").toString)
          .put("email", rowRDD.getAs("email").toString)
          .put("entitlementtoken", rowRDD.getAs("entitlementtoken").asInstanceOf[Int]))

      val data = sc
        //.parallelize(Seq(doc, doc2)) //this could be a list of documents to save as apposed to one!
        .parallelize(Seq(doc))
        .saveToCouchbase()
    })

  }

}


