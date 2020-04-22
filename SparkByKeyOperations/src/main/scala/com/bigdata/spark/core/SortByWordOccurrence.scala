/**
 * SortByWordOccurrence Program using Spark's By Key Operations / Transformations
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

object SortByWordOccurrence {
  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Long)]
   */
  def getSortByWordOccurrenceRDD(sparkSession: SparkSession): RDD[(String, Long)] =
    sparkSession
      .sparkContext
      .parallelize(Utils.getSortByWordOccurrenceInput, 3)
      .flatMap(x => x.split("\\W+"))
      .zipWithIndex()

  /**
   *
   * @param result - RDD[(String, Int, Long)]
   * @param methodType - String
   */
  def printResult(result: RDD[(String, Int, Long)], methodType: String): Unit = {
    println(s"\nUsing :: $methodType")
    println("-" * 50)
    result
      .collect()
      .sortBy(x => x._3)
      .map(x => (x._1, x._2))
      .foreach(println)
  }

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int, Long)]
   */
  def sortByOccurrenceUsingGroupBy(sparkSession: SparkSession): RDD[(String, Int, Long)] =
    getSortByWordOccurrenceRDD(sparkSession)
      .groupBy(x => x._1)
      .map(x => (x._1, x._2.toList.map(x => x._2)))
      .map(x => (x._1, x._2.size, x._2.min))

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int, Long)]
   */
  def sortByOccurrenceUsingGroupByKey(sparkSession: SparkSession): RDD[(String, Int, Long)] =
    getSortByWordOccurrenceRDD(sparkSession)
      .groupByKey()
      .map(x => (x._1, x._2.size, x._2.min))

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int, Long)]
   */
  def sortByOccurrenceUsingReduceByKey(sparkSession: SparkSession): RDD[(String, Int, Long)] =
    getSortByWordOccurrenceRDD(sparkSession)
      .map(x => (x._1, (1, x._2)))
      .reduceByKey((x, y) => (x._1 + y._1, math.min(x._2, y._2)))
      .map(x => (x._1, x._2._1, x._2._2))

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int, Long)]
   */
  def sortByOccurrenceUsingAggregateByKey(sparkSession: SparkSession): RDD[(String, Int, Long)] =
    getSortByWordOccurrenceRDD(sparkSession)
      .aggregateByKey(
        (0, Long.MaxValue))(
        (x, y) => (x._1 + 1, math.min(x._2, y)),
        (x, y) => (x._1 + y._1, math.min(x._2, y._2))
      )
      .map(x => (x._1, x._2._1, x._2._2))

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int, Long)]
   */
  def sortByOccurrenceUsingCombineByKey(sparkSession: SparkSession): RDD[(String, Int, Long)] =
    getSortByWordOccurrenceRDD(sparkSession)
      .combineByKey(
        (x: Long) => (1, x),
        (x: (Int, Long), y: Long) => (x._1 + 1, math.min(x._2, y)),
        (x: (Int, Long), y: (Int, Long)) => (x._1 + y._1, math.min(x._2, y._2))
      )
      .map(x => (x._1, x._2._1, x._2._2))

  /**
   *
   * @param args - Array[String]
   */
  def run(args: Array[String]): Unit = {
    val sparkSession = Utils.getSparkSession("SortByWordOccurrence", "local")
    sparkSession.sparkContext.setLogLevel("ERROR")

    printResult(sortByOccurrenceUsingGroupByKey(sparkSession), "GROUP BY KEY")
    printResult(sortByOccurrenceUsingReduceByKey(sparkSession), "REDUCE BY KEY")
    printResult(sortByOccurrenceUsingAggregateByKey(sparkSession), "AGGREGATE BY KEY")
    printResult(sortByOccurrenceUsingCombineByKey(sparkSession), "COMBINE BY KEY")
  }

  /**
   *
   * @param args - Array[String]
   */
  def main(args: Array[String]): Unit =
    run(args)
}
