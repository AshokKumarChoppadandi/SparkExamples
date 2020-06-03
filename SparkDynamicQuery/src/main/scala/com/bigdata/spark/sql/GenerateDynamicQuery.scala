package com.bigdata.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode}

import scala.collection.mutable.ListBuffer

object GenerateDynamicQuery {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("GenerateDynamicQuery")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    val inputColumnsFile = args(0)
    val outputFileLocation = args(1)

    val columns = sparkSession.read.option("header", value = true).csv(inputColumnsFile)

    val df1 = columns
      .groupBy(col("database"), col("table"))
      .agg(collect_list("column").as("column_list"))

    val rdd1 = df1.map(group => {
      val dbName = group.getAs[String](0)
      val tableName = group.getAs[String](1)
      val temp = group.getAs[Seq[String]](2)

      val minMaxSelectString = temp.map(columnName => {
        s"""
           | concat("$columnName", ",", min($columnName), ",", max($columnName))
        """.stripMargin
      }).mkString(", \":\", ")

      val query =
        s"""
           | select "$dbName" as dbName, "$tableName" as tableName, split(concat(${minMaxSelectString}), ':') as split_col from $dbName.$tableName
      """.stripMargin

      query
    })

    var listBuffer = new ListBuffer[String]()
    rdd1.collect().foreach(query => {
      val queryResult = sparkSession
        .sql(query)
        .withColumn("exp_col", explode(col("split_col")))
        .select("dbName", "tableName", "exp_col")
      queryResult
        .collect()
        .map(x => x.getAs[String](0) + "," + x.getAs[String](1) + "," + x.getAs[String](2))
        .foreach(x => listBuffer += x)
    })

    val finalDf = listBuffer.toList.toDS
    finalDf.write.mode(SaveMode.Overwrite).text(outputFileLocation)
  }
}
