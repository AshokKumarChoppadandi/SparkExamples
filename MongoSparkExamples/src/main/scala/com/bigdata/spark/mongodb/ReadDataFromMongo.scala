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
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
      .master("local")
      .getOrCreate()

    val mongoDBURL = s"mongodb://${user}:${password}@${host}:${port}/"
    println("MongoDB URL :: " + mongoDBURL)

    val df = spark.read.format("mongo").option("uri", mongoDBURL).option("database", db).option("collection", collectionName).load()

    df.show()
    df.printSchema()

    df.write.mode(SaveMode.Append).json("C:\\Users\\PC\\IdeaProjects\\SparkExamples\\MongoSparkExamples\\src\\main\\resources\\output")
    println("Output Written Successfully...!!!")

  }

}
