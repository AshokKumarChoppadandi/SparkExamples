/**
 * AverageSalary Program using Spark's By Key Operations / Transformations
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

object AverageSalary {
  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(Int, Int)]
   */
  def getAverageSalaryInputRDD(sparkSession: SparkSession): RDD[(Int, Int)] = sparkSession
    .sparkContext
    .parallelize(Utils.getAverageSalaryInput, 3)
    .map(x => x.split("\\W+"))
    .map(x => (x(2).toInt, x(1).toInt))

  /**
   *
   * @param input - RDD[(Int, Int)]
   * @return - RDD[(Int, Double)]
   */
  def getAverageSalaryUsingGroupByKey(input: RDD[(Int, Int)]): RDD[(Int, Double)] =
    input
      .groupByKey()
      .map(x => (x._1, x._2.sum.toDouble / x._2.size))

  /**
   *
   * @param input - RDD[(Int, Int)]
   * @return - RDD[(Int, Double)]
   */
  def getAverageSalaryUsingReduceByKey(input: RDD[(Int, Int)]): RDD[(Int, Double)] =
    input
      .map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))

  /**
   *
   * @param input - RDD[(Int, Int)]
   * @return - RDD[(Int, Double)]
   */
  def getAverageSalaryUsingAggregateByKey(input: RDD[(Int, Int)]): RDD[(Int, Double)] =
    input
      .aggregateByKey((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))

  /**
   *
   * @param input - RDD[(Int, Int)]
   * @return - RDD[(Int, Double)]
   */
  def getAverageSalaryUsingCombineByKey(input: RDD[(Int, Int)]): RDD[(Int, Double)] =
    input
      .combineByKey(
        (x: Int) => (x, 1),
        (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
        (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
      )
      .map(x => (x._1, x._2._1.toDouble / x._2._2))

  /**
   *
   * @param output - RDD[(Int, Double)]
   * @param methodType - String
   */
  def printWordCount(output: RDD[(Int, Double)], methodType: String): Unit = {
    println(s"\nUSING: $methodType")
    println("-" * 30)

    output
      .collect()
      .foreach(println)
  }

  /**
   *
   * @param args - Array[String]
   */
  def run(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession("SparkByKeyOperations", "local")
    spark.sparkContext.setLogLevel("ERROR")

    printWordCount(getAverageSalaryUsingGroupByKey(getAverageSalaryInputRDD(spark)), "GROUP BY KEY")
    printWordCount(getAverageSalaryUsingReduceByKey(getAverageSalaryInputRDD(spark)), "REDUCE BY KEY")
    printWordCount(getAverageSalaryUsingAggregateByKey(getAverageSalaryInputRDD(spark)), "AGGREGATE BY KEY")
    printWordCount(getAverageSalaryUsingCombineByKey(getAverageSalaryInputRDD(spark)), "COMBINE BY KEY")
  }

  /**
   *
   * @param args - Array[String]
   */
  def main(args: Array[String]): Unit =
    run(args)
}
