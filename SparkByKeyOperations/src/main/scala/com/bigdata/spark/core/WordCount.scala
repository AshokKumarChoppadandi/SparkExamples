/**
 * WordCount Program using Spark's By Key Operations / Transformations
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

object WordCount {

  /**
   *
   * @param sparkSession - SparkSession
   * @return - RDD[(String, Int)]
   */
  def getWordCountInputRDD(sparkSession: SparkSession): RDD[(String, Int)] = sparkSession
    .sparkContext
    .parallelize(Utils.getWordCountInput, 3)
    .flatMap(x => x.split("\\W+"))
    .map(x => (x, 1))

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Int)]
   */
  def getWordCountUsingGroupByKey(input: RDD[(String, Int)]): RDD[(String, Int)] =
    input
      .groupByKey()
      .map(x => (x._1, x._2.size))

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Int)]
   */
  def getWordCountUsingReduceByKey(input: RDD[(String, Int)]): RDD[(String, Int)] =
    input
      .reduceByKey((x, y) => x + y)

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Int)]
   */
  def getWordCountUsingAggregateByKey(input: RDD[(String, Int)]): RDD[(String, Int)] =
    input
      .aggregateByKey(0)((x, y) => x + y, (x, y) => x + y)

  /**
   *
   * @param input - RDD[(String, Int)]
   * @return - RDD[(String, Int)]
   */
  def getWordCountUsingCombineByKey(input: RDD[(String, Int)]): RDD[(String, Int)] =
    input
      .combineByKey((x: Int) => 1, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)

  /**
   *
   * @param output - RDD[(String, Int)]
   * @param methodType - String
   */
  def printWordCount(output: RDD[(String, Int)], methodType: String): Unit = {
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

    printWordCount(getWordCountUsingGroupByKey(getWordCountInputRDD(spark)), "GROUP BY KEY")
    printWordCount(getWordCountUsingReduceByKey(getWordCountInputRDD(spark)), "REDUCE BY KEY")
    printWordCount(getWordCountUsingAggregateByKey(getWordCountInputRDD(spark)), "AGGREGATE BY KEY")
    printWordCount(getWordCountUsingCombineByKey(getWordCountInputRDD(spark)), "COMBINE BY KEY")
  }

  /**
   *
   * @param args - Array[String]
   */
  def main(args: Array[String]): Unit = {
    run(args)
  }
}
