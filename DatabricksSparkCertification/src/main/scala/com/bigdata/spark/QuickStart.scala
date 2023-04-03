package com.bigdat.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuickStart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("QuickStart").master("local").getOrCreate()
    val list = (1 to 10).toList
    println(s"List :: ${list}")

    // RDD Operations
    val rdd1 = spark.sparkContext.parallelize(list, 3)
    val rdd2 = rdd1
      .map(x => x * x)
      .filter(x => x % 2 == 0)

    rdd2.collect().foreach(println)

    // Datasets
    // Reading a File
    val df = spark.read.textFile("src/main/resources/input/word-count.txt")
    df.show()

    // 1. Quick Start - Spark Basics
    // count, first & head
    println(df.count)
    println(df.first)
    println(df.head)

    // filter
    val linesWithSpark = df.filter(x => x.contains("Spark"))
    linesWithSpark.show(false)

    // line with more words
    import spark.implicits._
    val lineWithMoreWords = df
      .map(x => x.split(" ").length)
      .reduce((a, b) => Math.max(a, b))
    println(lineWithMoreWords)

    // WordCount
    val wordCount = df
      .select(
        explode(
          split(col("value"), "\\W+")).as("word"))
      .filter(length(col("word")) =!= 0)
      .groupBy("word")
      .count()
      .orderBy(desc("count"))

    wordCount.show()

  }
}
