package com.bigdata.spark.scala.window.functions

import com.bigdata.spark.models.Salary
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, cume_dist, lag, lead}

/**
  * Created by chas6003 on 24-04-2019.
  */
object AnalyticWindowFunctionsScalaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AnalyticWindowFunctionsScalaExample").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

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

    val windowSpec = Window.partitionBy("depName").orderBy(asc("salary"))

    // 1. Cummulative Distribution Window Function
    val cumeDistBySalary = cume_dist().over(windowSpec)
    val result1 = employeeSalaries.withColumn("cume_dist_by_salary", cumeDistBySalary)
    result1.show()

    // 2. Lead Window Function
    val leadBySalary = lead("salary", 1).over(windowSpec)
    val result2 = employeeSalaries.withColumn("lead_salary", leadBySalary)
    result2.show()

    // 3. Lag Window Function
    val lagBySalary = lag("salary", 1).over(windowSpec)
    val result3 = employeeSalaries.select('*, lagBySalary.as("lag_salary"))
    result3.show()

    // All Analytic Window Functions
    val cumeDistBySalary2 = cume_dist().over(windowSpec)
    val leadBySalary2 = lead("salary", 1).over(windowSpec)
    val lagBySalary2 = lag("salary", 1).over(windowSpec)

    val finalResult1 = employeeSalaries.select(
      '*,
      cumeDistBySalary2 as "cume_dist_by_salary",
      leadBySalary2 as 'lead_salary,
      lagBySalary2.as("lag_salary")
    )
    finalResult1.show()

    val finalResult2 = employeeSalaries
      .withColumn("cume_dist_by_salary", cumeDistBySalary2)
      .withColumn("lead_salary", leadBySalary2)
      .withColumn("lag_salary", lagBySalary2)
    finalResult2.show()
  }
}
