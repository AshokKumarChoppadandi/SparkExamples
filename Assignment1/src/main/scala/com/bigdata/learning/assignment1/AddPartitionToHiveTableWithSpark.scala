package com.bigdata.learning.assignment1

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object AddPartitionToHiveTableWithSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark csv app")
      .setMaster("local")
    val sc  = new SparkContext(conf)

    val year = 2019
    val month = "dec"

    val hiveContext = new HiveContext(sc)
    val query =
      s"""
        | ALTER TABLE default.std_temp1 ADD PARTITION (jyear = ${year}, jmonth = "${month}")
        | LOCATION "/user/cloudera/emp4"
      """.stripMargin

    hiveContext.sql(query)
    println("PARTITION ADDED SUCCESSFULLY...!!!")
  }
}
