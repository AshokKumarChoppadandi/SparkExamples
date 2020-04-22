/**
 * Input Data Preparation
 *
 * Created by Ashok Kumar Choppadandi on 21-Apr-2020.
 */

package com.bigdata.spark.core

import org.apache.spark.sql.SparkSession

object Utils {

  /**
   *
   * @param appName - String
   * @param master - String
   * @return SparkSession
   */
  def getSparkSession(appName: String, master: String): SparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()

  /**
   *
   * @return - List[String]
   */
  def getWordCountInput = List(
    "hadoop hadoop spark",
    "kafka cassandra hbase",
    "hadoop hadoop spark",
    "kafka cassandra hbase hadoop hadoop spark hadoop hadoop spark",
    "hadoop hadoop spark",
    "kafka cassandra hbase",
    "hadoop hadoop spark",
    "kafka cassandra hbase hadoop hadoop spark hadoop hadoop spark",
    "hadoop hadoop spark",
    "kafka cassandra hbase",
    "hadoop hadoop spark",
    "kafka cassandra hbase",
    "hadoop hadoop spark hadoop hadoop spark",
    "kafka cassandra hbase",
    "kafka cassandra hbase"
  )

  /**
   *
   * @return - List[String]
   */
  def getAverageSalaryInput = List(
    "Alice,10000,101",
    "Bob,20000,102",
    "Charlie,25000,101",
    "David,10000,102",
    "Gary,15000,101",
    "Henry,12000,103"
  )

  /**
   *
   * @return - List[String]
   */
  def getSortByWordOccurrenceInput = List(
    "Hi Hello Hi Hello",
    "Hello Namasthey",
    "Hello Spark, Unit testing Spark"
  )

  def getAverageDistanceTravelledInput = "src/main/resources/flights.csv"
}
