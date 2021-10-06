package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FilesLineSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FilesLineSum").master("local").getOrCreate()

    val filePath1 = "./src/main/resources/FileSumInput/InputFile_1.txt"
    val filePath2 = "./src/main/resources/FileSumInput/InputFile_2.txt"

    val df11 = spark.read.textFile(filePath1)
    val df21 = spark.read.textFile(filePath2)

    val df12 = df11
      .withColumn("id", monotonically_increasing_id())

    val df22 = df21
      .withColumn("id", monotonically_increasing_id())

    val unionDf = df12.union(df22)

    df12.printSchema()
    df22.printSchema()

    /*df12.where(col("id") >= 999980).show()
    df22.where(col("id") >= 999980).show()*/
    val explodeDf = unionDf
      .select(col("id"), explode(split(col("value"), " ").cast("array<int>")).as("value_as_ints"))
      .repartition(col("id"))

    val finalDf = explodeDf.groupBy("id").agg(sum("value_as_ints").as("sum_of_ints"))
    finalDf.show()


  }
}
