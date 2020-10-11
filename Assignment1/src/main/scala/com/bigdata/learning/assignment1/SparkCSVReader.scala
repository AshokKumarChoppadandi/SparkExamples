package com.bigdata.learning.assignment1

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object SparkCSVReader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark csv app")
      .setMaster("local")
    val sc  = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
     * 10,lakshmi,ulise,2000,12-06-1988,101,dec,2019
     * 20,john,boss,3000,04-09-1990,102,dec,2019
     */

    val optionsMap = Map(
      "header" -> "true",
      "inferSchema" -> "true"
    )

    val csvDF = sqlContext
      .read
      .format("csv")
      .options(optionsMap)
      .load("/home/cloudera/Desktop/spark.csv")

    csvDF.printSchema()
    csvDF.show()

    /*csvDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("jmonth", "jyear")
      .parquet("/user/cloudera/emp4")*/

    println("DATA WRITTEN SUCCESSFULLY...!!!")
  }
}
