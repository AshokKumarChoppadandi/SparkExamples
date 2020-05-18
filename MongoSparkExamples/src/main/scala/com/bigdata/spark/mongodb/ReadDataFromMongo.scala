package com.bigdata.spark.mongodb

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadDataFromMongo {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("application.conf").getConfig("com.mongodb")

    val host = config.getString("hostname")
    val port = config.getInt("port")
    val user = config.getString("user")
    val password = config.getString("password")
    val db = config.getString("db")
    val collectionName = config.getString("collection")


    val spark = SparkSession
      .builder()
      .appName("ReadDataFromMongo")
      .master("local")
      .getOrCreate()

    val mongoDBURL = s"mongodb://${user}:${password}@${host}:${port}/"
    println("MongoDB URL :: " + mongoDBURL)

    val df = spark.read.format("mongo").option("uri", mongoDBURL).option("database", db).option("collection", collectionName).load()

    df.show()
    df.printSchema()

    df.write.mode(SaveMode.Append).json("/Users/achoppadandi/IdeaProjects/MongoSparkExamples/src/main/resources/output")
    println("Output Written Successfully...!!!")

  }

}
