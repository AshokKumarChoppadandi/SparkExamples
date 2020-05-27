/**
 * Date and Timestamp formats :
 *
 * https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
 */

package com.bigdata.spark.mongodb

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object ReadIncrementalDataFromMongo {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("application.conf").getConfig("com.mongodb")

    val host = config.getString("hostname")
    val port = config.getInt("port")
    val user = config.getString("user")
    val password = config.getString("password")
    val db = config.getString("db")
    val collectionName = config.getString("collection")

    val lastUpdatedTimestampFilePath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("ReadIncrementalDataFromMongo")
      .master("local")
      .getOrCreate()

    val timestampAsString = spark.read.textFile(lastUpdatedTimestampFilePath).first().trim

    val mongoDBURL = s"mongodb://${user}:${password}@${host}:${port}/"
    println("MongoDB URL :: " + mongoDBURL)

    val df = spark.read.format("mongo").option("uri", mongoDBURL).option("database", db).option("collection", collectionName).load()

    val df2 = if(timestampAsString.length != 0 && timestampAsString != null && !timestampAsString.equals("null")) {
      df.filter(col("_id.oid").>(timestampAsString))
    } else {
      df
    }

    df2.show(false)
    df2.printSchema()

    df2.write.mode(SaveMode.Overwrite).json(outputPath)
    println("Output Written Successfully...!!!")

    val lastTimestampWritten = df2.select(max(col("_id.oid")))
    lastTimestampWritten.show(false)
    lastTimestampWritten.coalesce(1).write.mode(SaveMode.Overwrite).text(lastUpdatedTimestampFilePath)
    println("Last Read ObjectId is written successfully to file...!!!")

  }

  def getLastRetrievedTimestamp: String => Date = (timestampAsString: String) => {
    val format = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)"
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.parse(timestampAsString)
  }
}
