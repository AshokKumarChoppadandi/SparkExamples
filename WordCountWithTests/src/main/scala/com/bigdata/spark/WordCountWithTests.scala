package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
/**
  * Created by Ashok Kumar on 04-05-2017.
  */
object WordCountWithTests {

  var tempVar = "ashok"
  def wordCounts(inputRdd: RDD[String]): RDD[(String, Int)] = {
    val wc: RDD[(String, Int)] = inputRdd.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wc
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
}
