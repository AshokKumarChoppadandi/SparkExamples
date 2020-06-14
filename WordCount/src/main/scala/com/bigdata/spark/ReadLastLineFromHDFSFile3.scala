/**
 * spark-submit --master local --class com.bigdata.spark.ReadLastLineFromHDFSFile3 target/scala-2.11/WordCount-assembly-0.1.jar /user/hadoop/EDGARLogs/log20170630.csv Trailer
 */

package com.bigdata.spark

import java.util.Date

import org.apache.spark.sql.{Encoders, SparkSession}

object ReadLastLineFromHDFSFile3 {
  def main(args: Array[String]): Unit = {

    println("Start Time :: " + new Date())
    val filePath = args(0)
    val searchCode = args(1)
    val spark = SparkSession.builder()/*.master("local")*/.appName("ReadLastLineFromHDFSFile2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.textFile(filePath)
    val partitions = df.rdd.getNumPartitions
    println("Number of partitions :: " + partitions)

    val df2 = df.mapPartitions(x => {
      val tmpList = x.toList
      val lastValue = tmpList.last //tmpList(tmpList.size - 1)
      List(lastValue).toIterator
    })(Encoders.STRING)

    df2.cache()

    val countAfterMapPartitions = df2.count()
    println("Count After Map Partitions :: " + countAfterMapPartitions)

    val result = df2.filter(x => x.startsWith(searchCode)).coalesce(1)

    //result.show()

    if(result.count() > 0) {
      val line = result.first()
      val lineArr = line.split(",")
      val requiredCode = lineArr(0)
      val requiredDate = lineArr(1)
      val requiredCount = lineArr(2)

      println("Code :: " + requiredCode)
      println("Date :: " + requiredDate)
      println("Count :: " + requiredCount)
    } else {
      println(s"No data exists for the given search code : $searchCode")
    }

    println("End Time :: " + new Date())
  }
}
