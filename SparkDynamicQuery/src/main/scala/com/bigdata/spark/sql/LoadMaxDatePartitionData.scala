package com.bigdata.spark.sql

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object LoadMaxDatePartitionData {

  val dateString = "YYYY-MM-dd"
  val dateFormat = new SimpleDateFormat(dateString)

  def main(args: Array[String]): Unit = {

    val inputDbName = args(0)
    val inputTableName = args(1)
    val partitionColumn = args(2)
    val outputDbName = args(3)
    val outputTableName = args(4)

    val spark = SparkSession
      .builder()
      .appName("LoadMaxDatePartitionData")
      .master("local")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val currentDate = dateFormat.format(new Date())
    val maxDate = spark
      .read
      .table(s"$inputDbName.$inputTableName")
      .select(max(col(partitionColumn)))

    if(currentDate.equals(maxDate.first().getAs[String](0))) {
      println("Dates Matched with the Current, Hence loading the data...!!!")

      spark
        .read
        .table(s"$inputDbName.$inputTableName")
        .filter(col("dt") === currentDate)
        .write
        .partitionBy(partitionColumn)
        .mode(SaveMode.Append)
        .format("hive")
        .saveAsTable(s"$outputDbName.$outputTableName")
    } else {
      println("Dates not Matched, Hence aborting the job...!!!")
    }
  }
}
