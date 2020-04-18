/**
 * This is the Spark program which has 13 variations of writing WordCount using RDD, DataFrame and DataSet APIs
 * Created by Ashok Kumar Choppadandi on 18-Apr-2020.
 */
package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc, explode, split}

import scala.collection.mutable.ListBuffer
import Constants._
object WordCount {

  /**
   * Initialising the SparkSession object
   * ------------------------------------
   *
   * @param appName - Application Name
   * @param master - Spark Master (local, spark, yarn)
   * @return - SparkSession
   */
  def getSparkSession(appName: String, master: String) =  SparkSession.builder().appName(appName).master(master).getOrCreate()

  /**
   * Preparing test input for WordCount program
   * ------------------------------------------
   *
   * @return - List[String]
   */
  def getInputData = List(
    "Hi Hello Hi Hello",
    "Hello Namasthey",
    "Hello Spark, Unit testing Spark"
  )

  /**
   * WordCount: 1 - RDD : Using FlatMap, Map & ReduceByKey
   * -----------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount1(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd1 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount1 = rdd1
      .flatMap(x => x.split(WORD_SPLITTER))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(x => ~x._2)

    wordCount1.collect()
  }

  /**
   * WordCount: 2 - RDD : Using FlatMap & CountByValue
   * -------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - scala.collection.Map[java.lang.String, Long]
   */
  def getWordCount2(sparkSession: SparkSession): scala.collection.Map[java.lang.String, Long] = {
    val rdd2 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount2 = rdd2.flatMap(x => x.split(WORD_SPLITTER))
      .countByValue()

    wordCount2
  }

  /**
   * WordCount: 3 - RDD : Using FlatMap & CountByKey
   * -----------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - scala.collection.Map[java.lang.String, Long]
   */
  def getWordCount3(sparkSession: SparkSession): scala.collection.Map[java.lang.String, Long] = {
    val rdd3 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount3 = rdd3.flatMap(x => x.split(WORD_SPLITTER))
      .map(x => (x, null))
      .countByKey()
    wordCount3
  }

  /**
   * WordCount: 4 - RDD : Using FlatMap, GroupBy & Map
   * -------------------------------------------------
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount4(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd4 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount4 = rdd4.flatMap(x => x.split(WORD_SPLITTER))
      .groupBy(x => x)
      .map(x => (x._1, x._2.size))
      .sortBy(x => ~x._2)

    wordCount4.collect()
  }

  /**
   * WordCount: 5 - RDD : Using FlatMap, Map & AggregateByKey
   * --------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount5(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd5 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount5 = rdd5.flatMap(x => x.split(WORD_SPLITTER))
      .map(x => (x, 1))
      .aggregateByKey(0)((x, y) => x + 1, (x, y) => x + y)
      .sortBy(x => ~x._2)

    wordCount5.collect()

  }

  /**
   * WordCount: 6 - RDD : Using FlatMap, Map & CombineByKey
   * ------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount6(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd6 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount6 = rdd6.flatMap(x => x.split(WORD_SPLITTER))
      .map(x => (x, 1))
      .combineByKey((x: Int) => 0, (x: Int, y: Int) => x + 1, (x: Int, y: Int) => x + y)
      .sortBy(x => ~x._2)

    wordCount6.collect()
  }

  /**
   * WordCount: 7 - RDD : Using FlatMap, Map & FoldByKey
   * ---------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount7(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd7 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount7 = rdd7.flatMap(x => x.split(WORD_SPLITTER))
      .map(x => (x, 1))
      .foldByKey(0)((x, y) => x + y)
      .sortBy(x => ~x._2)

    wordCount7.collect()
  }

  /**
   * WordCount: 8 - RDD : Using MapPartitions, Map & ReduceByKey
   * -----------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount8(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd8 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount8 = rdd8.mapPartitions(x => x.flatMap(y => y.split(WORD_SPLITTER))
      .map(x => (x, 1)))
      .reduceByKey((x, y) => x + y)
      .sortBy(x => ~x._2)

    wordCount8.collect()
  }

  /**
   * WordCount: 9 - RDD : Using MapPartitions, Map & ReduceByKey
   * -----------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[(String, Int)]
   */
  def getWordCount9(sparkSession: SparkSession): Array[(String, Int)] = {
    val rdd9 = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount9 = rdd9.mapPartitions(x => {
      val buffer = new ListBuffer[String]()
      x.foreach(y => {
        val tmp = y.split(WORD_SPLITTER)
        buffer ++= tmp.toList
      })
      buffer.toList.toIterator
    })
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(x => ~x._2)

    wordCount9.collect()
  }

  /**
   * WordCount: 10 - DataFrame : Using Select, Explode, GroupBy & Count
   * ------------------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[Row]
   */
  def getWordCount10(sparkSession: SparkSession): Array[Row] = {
    import sparkSession.implicits._

    val wordCount10 = getInputData
      .toDF(TEXT)
      .select(explode(split(col(TEXT), WORD_SPLITTER)).as(WORD))
      .groupBy(WORD)
      .count()
      .orderBy(desc(COUNT))

    wordCount10.collect()
  }

  /**
   * WordCount: 11 - DataFrame : Using Transform, Select, Explode, GroupBy & Count
   * -----------------------------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[Row]
   */
  def getWordCount11(sparkSession: SparkSession): Array[Row] = {
    import sparkSession.implicits._

    val wordCount11 = getInputData
      .toDF(TEXT)
      .transform(transformMethod1)
    wordCount11.collect()
  }

  /**
   * Transform Method: 1
   * -------------------
   *
   * @param df - DataFrame
   * @return - DataFrame
   */
  def transformMethod1(df: DataFrame): DataFrame = {
    df.select(explode(split(col(TEXT), WORD_SPLITTER)).as(WORD))
      .groupBy(WORD)
      .count()
      .orderBy(desc(COUNT))
  }

  /**
   * WordCount: 12 - DataFrame : TempTable & SQL Query
   * -------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[Row]
   */
  def getWordCount12(sparkSession: SparkSession): Array[Row] = {
    import sparkSession.implicits._

    getInputData
      .toDF(TEXT)
      .createOrReplaceTempView(WORD_COUNT_TEMP_TABLE)

    val wordCount12 = sparkSession.sql(
      s"""
        | select word, count(${WORD}) from (
        |   select explode(split(${TEXT}, '${WORD_SPLITTER}')) as word from ${WORD_COUNT_TEMP_TABLE}
        | ) tmp group by ${WORD}
        | order by ${WORD}
      """.stripMargin)

    wordCount12.collect()
  }

  /**
   * WordCount: 13 - Dataset : Using FlatMap, GroupBy & Count
   * --------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[Row]
   */
  def getWordCount13(sparkSession: SparkSession): Array[Row] = {
    val rdd: RDD[String] = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount13 = sparkSession.createDataset(rdd)(Encoders.STRING)
      .flatMap(x => x.split(WORD_SPLITTER))(Encoders.STRING)
      .groupBy(VALUE)
      .count()
      .orderBy(desc(COUNT))

    wordCount13.collect()
  }

  /**
   * WordCount: 14 - Dataset : Using Transform, FlatMap, GroupBy & Count
   * ---------------------------------------------------------------------
   *
   * @param sparkSession - SparkSession
   * @return - Array[Row]
   */
  def getWordCount14(sparkSession: SparkSession): Array[Row] = {
    val rdd: RDD[String] = sparkSession.sparkContext.parallelize(getInputData, 3)
    val wordCount14 = sparkSession.createDataset(rdd)(Encoders.STRING)
      .transform(transformMethod2)
    wordCount14.collect()
  }

  /**
   *
   * Transform Method: 2
   * -------------------
   *
   * @param ds - Dataset[String]
   * @return - Dataset[Row]
   */
  def transformMethod2(ds: Dataset[String]): Dataset[Row] = {
    ds.flatMap(x => x.split(WORD_SPLITTER))(Encoders.STRING)
      .groupBy(VALUE)
      .count()
      .orderBy(desc(COUNT))
  }

  /**
   * Main Method
   * -----------
   *
   * @param args - Array[String]
   */
  def main(args: Array[String]): Unit = {

    val sparkSession = getSparkSession(APP_NAME, MASTER)

    getWordCount1(sparkSession).foreach(println)
    getWordCount2(sparkSession).foreach(println)
    getWordCount3(sparkSession).foreach(println)
    getWordCount4(sparkSession).foreach(println)
    getWordCount5(sparkSession).foreach(println)
    getWordCount6(sparkSession).foreach(println)
    getWordCount7(sparkSession).foreach(println)
    getWordCount8(sparkSession).foreach(println)
    getWordCount9(sparkSession).foreach(println)
    getWordCount10(sparkSession).foreach(println)
    getWordCount11(sparkSession).foreach(println)
    getWordCount12(sparkSession).foreach(println)
    getWordCount13(sparkSession).foreach(println)
    getWordCount14(sparkSession).foreach(println)

  }
}
