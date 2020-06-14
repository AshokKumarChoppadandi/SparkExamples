package com.bigdata.spark

import org.apache.spark.sql.SparkSession

import sys.process._

object ReadLastLineFromHDFSFile1 {
  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val tailCommand = s"hdfs dfs -tail $filePath"

    val result = tailCommand !!

    val resultArr = result.split("\\n")
    val lastLine = resultArr(resultArr.length - 1)

    val lastLineArr = lastLine.split("\\|")

    val code = lastLineArr(0)
    val date = lastLineArr(1)
    val count = lastLineArr(2)

    println("Code is :: " + code)
    println("Date is :: " + date)
    println("Count is :: " + count)

    if(date.equals("2020-06-12")) {
      val spark = SparkSession.builder().master("local").appName("FilterPartitionsExample").getOrCreate()

      val df = spark
        .read
        .option("header", "true")
        .option("delimiter", "|")
        .csv(args(0))

      df.show()
    } else {
      println("Spark program has not started...!!!")
    }
  }
}
