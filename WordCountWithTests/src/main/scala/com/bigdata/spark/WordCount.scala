package com.bigdata.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
/**
  * Created by Ashok Kumar on 04-05-2017.
  */
class WordCount {

  def wordCounts(sc: SparkContext, url: String): RDD[(String, Int)] = {
    val inputRdd = sc.textFile(url)
    val wc: RDD[(String, Int)] = inputRdd.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wc
  }

  def getDf(sQLContext: SQLContext, path: String): DataFrame = {

    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    val data = sQLContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = sQLContext.createDataFrame(rdd, schema)
    df
  }

  def getTempTable(sQLContext: SQLContext, path: String): String = {

    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    val data = sQLContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = sQLContext.createDataFrame(rdd, schema)
    val tabName = "Test"
    df.registerTempTable(tabName)
    tabName
  }

  def getHiveTable (hiveContext: HiveContext, path: String): String = {
    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    val data = hiveContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = hiveContext.createDataFrame(rdd, schema)
    val tabName = "HiveTable"
    df.registerTempTable(tabName)

    tabName
  }

  def getHiveDBTable(hiveContext: HiveContext, path: String, dbName: String): String = {
    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    val data = hiveContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    hiveContext.sql(s"create database if not exists $dbName")
    hiveContext.sql(s"use $dbName")
    val df = hiveContext.createDataFrame(rdd, schema)
    val tabName = "HiveTable"
    df.registerTempTable(tabName)

    tabName
  }
}
