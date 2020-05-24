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
      .appName("ReadDataFromMongo")
      .master("local")
      .getOrCreate()

    val timestampAsString = "Read From a File"//spark.read.textFile(lastUpdatedTimestampFilePath).first()
    val lastRetrievedTimestamp = getLastRetrievedTimestamp(timestampAsString)

    val mongoDBURL = s"mongodb://${user}:${password}@${host}:${port}/"
    println("MongoDB URL :: " + mongoDBURL)

    val df = spark.read.format("mongo").option("uri", mongoDBURL).option("database", db).option("collection", collectionName).load()
    val df2 = df.filter(col("submittedTime").>(lastRetrievedTimestamp))

    df2.show()
    df2.printSchema()

    df2.write.mode(SaveMode.Append).json(outputPath)
    println("Output Written Successfully...!!!")

    val lastTimestampWritten = df2.select(max(col("submittedTime")))
    // Write it to a file.

  }

  def getLastRetrievedTimestamp(timestampAsString: String): Date = {
    val format = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)"
    val dateFormat = new SimpleDateFormat(format)

    dateFormat.parse(timestampAsString)
  }
}
