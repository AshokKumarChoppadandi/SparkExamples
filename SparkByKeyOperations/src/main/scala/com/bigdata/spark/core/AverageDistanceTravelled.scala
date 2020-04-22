/**
 * AverageDistanceTravelled by a Flight Program using Spark's By Key Operations / Transformations
 *  1. GroupByKey
 *  2. ReduceByKey
 *  3. AggregateByKey
 *  4. CombineByKey
 *
 * Created by Ashok Kumar Choppadandi on 21-Apr-2020.
 */
package com.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AverageDistanceTravelled {

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int)]
   */
  def getAverageDistanceTravelledInputRDD(sparkSession: SparkSession): RDD[(String, Int)] =
    sparkSession.sparkContext.textFile(Utils.getAverageDistanceTravelledInput, 5)
      .map(x => x.split(","))
      .map(x => (x(5), x(17).toInt))

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Double)]
   */
  def getAverageDistanceTravelledUsingGroupByKey(input: RDD[(String, Int)]): RDD[(String, Double)] = {
    input.groupByKey()
      .map(x => (x._1, x._2.sum, x._2.size))
      .map(x => (x._1, x._2.toDouble / x._3))
  }

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Double)]
   */
  def getAverageDistanceTravelledUsingReduceByKey(input: RDD[(String, Int)]): RDD[(String, Double)] = {
    input
      .map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
  }

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Double)]
   */
  def getAverageDistanceTravelledUsingAggregateByKey(input: RDD[(String, Int)]): RDD[(String, Double)] = {
    input
      .aggregateByKey((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
  }

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Double)]
   */
  def getAverageDistanceTravelledUsingCombineByKey(input: RDD[(String, Int)]): RDD[(String, Double)] = {
    input
      .combineByKey(
        (x: Int) => (x, 1),
        (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
        (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
      )
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
  }

  /**
   *
   * @param output - RDD[(String, Double)]
   * @param methodType - String
   */
  def printAverageDistanceTravelled(output: RDD[(String, Double)], methodType: String): Unit = {
    println(s"\nTop 10 Flights with Average Distance Travelled - Using $methodType Operation")
    println("-" * 50)
    output
      .sortBy(x => x._2, ascending = false)
      .take(10)
      .foreach(println)
  }

  /**
   *
   * @param args - Array[String]
   */
  def run(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession("AverageDistanceTravelled", "local")
    spark.sparkContext.setLogLevel("ERROR")

    val inputRDD = getAverageDistanceTravelledInputRDD(spark)

    printAverageDistanceTravelled(getAverageDistanceTravelledUsingGroupByKey(inputRDD), "GROUP BY KEY")
    printAverageDistanceTravelled(getAverageDistanceTravelledUsingReduceByKey(inputRDD), "REDUCE BY KEY")
    printAverageDistanceTravelled(getAverageDistanceTravelledUsingAggregateByKey(inputRDD), "AGGREGATE BY KEY")
    printAverageDistanceTravelled(getAverageDistanceTravelledUsingCombineByKey(inputRDD), "COMBINE BY KEY")

  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}
