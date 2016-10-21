import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.spark._
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

object MYSQL_to_Couchbase {
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
    entitlements_dataFrame.show()
    entitlements_dataFrame.printSchema()
    entitlements_dataFrame.schema.foreach(println)
    entitlements_dataFrame.schema.foreach(x => println(x.name) + " " + println(x.dataType))
    entitlements_dataFrame.registerTempTable("names")
    val tableSchema = entitlements_dataFrame.schema

    /* loading this:
      +---------+-----------+-----------------+----------------+
      |givenname|    surname|            email|entitlementtoken|
      +---------+-----------+-----------------+----------------+
      |     Matt| Ingenthron|   matt@email.com|           11211|
      |  Michael|Nitschinger|michael@email.com|           11210|
      +---------+-----------+-----------------+----------------+
     */

    val rowsList = entitlements_dataFrame.sqlContext.sql("select * from names limit 10").collect()
    //run as RDD - which will parallelize processing across threads :-D
    //    val rowsListRDD = sc.parallelize(rowsList)
    //    rowsListRDD.foreach(rowRDD => {
    //      println("TERRY - FOREACH, writing rowRDD to Couchbase " + rowRDD + " on " + Thread.currentThread().getName)
    //            val doc = JsonDocument.create(rowRDD.getAs("email").toString,JsonObject.empty()
    //              .put("givenname", rowRDD.getAs("givenname").toString)
    //              .put("surname", rowRDD.getAs("surname").toString)
    //              .put("email", rowRDD.getAs("email").toString)
    //              .put("entitlementtoken", rowRDD.getAs("entitlementtoken").asInstanceOf[Int])
    //              .put("DateOfBirth",if(rowRDD.getAs("DateOfBirth") != null) rowRDD.getAs("DateOfBirth").toString else "null")
    //            )
    //      val data = sc
    //        .parallelize(Seq(doc))
    //        .saveToCouchbase()
    //    })


    val rowsListRDD = sc.parallelize(rowsList)
    rowsListRDD.foreach(rowRDD => {
      println("TERRY - FOREACH, writing rowRDD to Couchbase " + rowRDD + " on " + Thread.currentThread().getName)

      val jsonObject = JsonObject.empty()

      //simple table schema cannot support objects
      tableSchema.foreach(i => jsonObject.put(i.name,
        if (rowRDD.getAs(i.name) != null) {
          //rowRDD.getAs(i.name).toString
          if(i.dataType == org.apache.spark.sql.types.DataTypes.StringType)
            rowRDD.getAs(i.name).toString
          else if(i.dataType == org.apache.spark.sql.types.DataTypes.IntegerType)
            rowRDD.getAs(i.name).asInstanceOf[Int]
          else if(i.dataType == org.apache.spark.sql.types.DataTypes.BooleanType)
            rowRDD.getAs(i.name).asInstanceOf[Boolean]
          else
            rowRDD.getAs(i.name).toString //need a default
        }
        else
          "NULL"
      ))

      println(jsonObject)

      val doc = JsonDocument.create(rowRDD.getAs("email").toString, jsonObject)

      val data = sc
        .parallelize(Seq(doc))
        .saveToCouchbase()
    })

  }
}
