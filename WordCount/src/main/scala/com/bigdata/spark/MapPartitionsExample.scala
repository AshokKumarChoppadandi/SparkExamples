/**
 * Problem Statement:
 *
 * 1. RDD of Strings which includes the Integer and String values.
 * 2. To add all the Numbers in each partition
 * 3. If any one of the value in the partition is String and unable to convert to Integer,
 *    then the sum of that partition should be NULL.
 *
 * Input:
 *
 * List(100, 200, 300, 150, 250, 350, 300, test, 100)
 *
 * Let's say after we parallelizing the data in Spark looks like below.
 *
 * Example:
 *
 * Using Map Partition ---------------> (Expected Output)
 * ----------------------------------------------------
 * Partition   |    Values        |         Sum
 * ----------------------------------------------------
 *     0       |  100, 200, 300   |         600
 *     1       |  150, 250, 350   |         750
 *     2       |  300, test, 100  |         NULL
 * ----------------------------------------------------
 *
 * Using Map Partitions with Key -----> (Expected Output)
 * ----------------------------------------------------
 * Partition   |    Values        |         Sum
 * ----------------------------------------------------
 *     0       |  100, 200, 300   |         (0, 600)
 *     1       |  150, 250, 350   |         (1, 750)
 *     2       |  300, test, 100  |         (2, NULL)
 * ----------------------------------------------------
 *
 */

package com.bigdata.spark

import org.apache.spark.sql.SparkSession

object MapPartitionsExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("MapPartitionsExample")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val list = List(
      "100", "150",
      "200", "250", "300",
      "test3", "400", "500"
    )

    val rdd = sparkSession.sparkContext.parallelize(list, 3)

    println("\nUse Case 1:\nSum of Numbers with in the Each Partition using `mapPartitionsWithIndex`:")
    val rdd2 = rdd.mapPartitionsWithIndex((key, x) => {

      val partitionSum = try {
        x.map(y => y.toInt).sum
      } catch {
        case _: Exception => null
      }

      List((key, partitionSum)).toIterator
    })

    rdd2.collect().foreach(println)

    println("\nUse Case 2:\nSum of Numbers with in the Each Partition using `mapPartitions`:")
    val rdd3 = rdd.mapPartitions(x => {

      val partitionSum = try {
        x.map(y => y.toInt).sum
      } catch {
        case _: Exception => null
      }

      List(partitionSum).toIterator
    })

    rdd3.collect().foreach(println)
  }
}
