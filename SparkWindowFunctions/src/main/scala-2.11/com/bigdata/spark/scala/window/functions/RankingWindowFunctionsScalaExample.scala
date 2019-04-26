package com.bigdata.spark.scala.window.functions

import com.bigdata.spark.models.Salary
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank, dense_rank, percent_rank, ntile, row_number}

/**
  * Created by chas6003 on 24-04-2019.
  */
object RankingWindowFunctionsScalaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RankingWindowFunctionsScalaExample").master("local").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val employeeSalaries = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)
    ).toDS

    val windowSpec = Window.partitionBy("depName").orderBy(desc("salary"))

    // 1. Rank Window Function
    val rankBySalary = rank().over(windowSpec)
    val result1 = employeeSalaries.select('*, rankBySalary as 'rank)
    result1.show()

    // 2. Dense Rank Window Function
    val denseRankBySalary = dense_rank().over(windowSpec)
    val result2 = employeeSalaries.select('*, denseRankBySalary as 'dense_rank)
    result2.show()

    // 3. Percent Rank Window Function
    val percentRankBySalary = percent_rank().over(Window.partitionBy("depName").orderBy(desc("salary")))
    val result3 = employeeSalaries.select('*, percentRankBySalary as 'percent_rank)
    result3.show()

    // 4. Ntile Window Function - This is used for generating the group id's
    val ntileBySalary = ntile(2).over(windowSpec)
    val result4 = employeeSalaries.select('*, ntileBySalary as 'ntile_sal)
    result4.show()

    // 5. Row Number Window Function
    val rowNumberBySalary = row_number().over(windowSpec)
    val result5 = employeeSalaries.withColumn("rn", rowNumberBySalary)
    result5.show()

    // All Ranking Window Functions
    val rankBySalary2 = rank().over(windowSpec)
    val denseRankBySalary2 = dense_rank().over(windowSpec)
    val percentRankBySalary2 = percent_rank().over(windowSpec)
    val ntileBySalary2 = ntile(2).over(windowSpec)
    val rowNumberBySalary2 = row_number().over(windowSpec)

    val finalResult1 = employeeSalaries
      .select(
        '*,
        rankBySalary2.as("rnk"),
        denseRankBySalary2.as("dense_rnk"),
        percentRankBySalary2.as("percent_rnk"),
        ntileBySalary2.as("ntile_group_id"),
        rowNumberBySalary2.as("rn")
      )
    finalResult1.show()

    val finalResult2 = employeeSalaries
      .withColumn("rnk", rankBySalary2)
      .withColumn("dense_rnk", denseRankBySalary2)
      .withColumn("percent_rnk", percentRankBySalary2)
      .withColumn("ntile_group_id", ntileBySalary2)
      .withColumn("rn", rowNumberBySalary2)
    finalResult2.show()
  }
}
