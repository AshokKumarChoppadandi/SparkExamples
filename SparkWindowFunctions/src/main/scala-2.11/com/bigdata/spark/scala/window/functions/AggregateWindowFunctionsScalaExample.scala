package com.bigdata.spark.scala.window.functions

import java.util.logging.Logger

import com.bigdata.spark.models.Salary
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, avg, min, max, count}

/**
  * Created by chas6003 on 24-04-2019.
  */
object AggregateWindowFunctionsScalaExample {
  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel("ERROR")
    val spark = SparkSession.builder().appName("AggregateWindowFunctionsScalaExample").master("local").getOrCreate()
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

    val byDepName = Window.partitionBy("depName")

    // 1. Average Window Function
    val result1 = employeeSalaries.withColumn("dept_avg_sal", avg("salary").over(byDepName))
    result1.show()

    val result2 = employeeSalaries.withColumn("dept_avg_sal", avg("salary") over byDepName)
    result2.show()

    // 2. Sum Window Function
    val result3 = employeeSalaries.withColumn("dept_total_sal", sum("salary").over(byDepName))
    result3.show()

    // 3. Min Aggregate Function
    val result4 = employeeSalaries.withColumn("dept_min_sal", min("salary").over(byDepName))
    result4.show()

    // 4. Max Aggregate Function
    val result5 = employeeSalaries.withColumn("dept_max_sal", max("salary").over(byDepName))
    result5.show()

    // 5. Count Aggregate Function
    val result6 = employeeSalaries.withColumn("num_of_dept_salaries", count("salary").over(byDepName))
    result6.show()

    // All Aggregate Functions

    val finalResult = employeeSalaries
      .withColumn("dept_avg_sal", avg("salary").over(byDepName))
      .withColumn("dept_total_sal", sum("salary").over(byDepName))
      .withColumn("dept_min_sal", min("salary").over(byDepName))
      .withColumn("dept_max_sal", max("salary").over(byDepName))
      .withColumn("num_of_dept_salaries", count("salary").over(byDepName))
    finalResult.show()

  }
}
