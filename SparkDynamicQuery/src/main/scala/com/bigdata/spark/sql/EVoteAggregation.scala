package com.bigdata.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable.Seq

/**
 *
 * Expected Input:
 *
 * BJP,INC,BSP,TEMP1,TEMP2
 * 0,0,0,temp1,temp2
 * 0,0,1,temp1,temp2
 * 1,0,1,temp1,temp2
 * 1,0,0,temp1,temp2
 * 1,1,1,temp1,temp2
 * 0,1,0,temp1,temp2
 * 1,1,1,temp1,temp2
 * 1,0,1,temp1,temp2
 * 1,0,1,temp1,temp2
 *
 * Expected Output:
 *
 * BJP Total Votes,INC Total Votes,BSP Total Votes
 * 6,3,6
 */

object EVoteAggregation {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("eVote Results")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val tempGroupColumn = "temp"
    val ignoreColumns = Seq("TEMP1", "TEMP2")

    val inputDf = spark
      .read
      .option("header", "true")
      .csv(inputPath)
      .drop(ignoreColumns:_*)

    val processCols = inputDf.columns
    val dataWithTempGroupColumn = inputDf.withColumn(tempGroupColumn, lit("universal_group"))

    val groupedDf = dataWithTempGroupColumn.groupBy(tempGroupColumn)
    val sumLogic = processCols.map(x => {
      sum(col(x).cast(DataTypes.LongType)).as(s"${x} Total Votes")
    })

    val finalResult = groupedDf.agg(
      sumLogic.head,
      sumLogic.slice(1, sumLogic.length): _*
    )

    finalResult
      .drop(tempGroupColumn)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }
}

