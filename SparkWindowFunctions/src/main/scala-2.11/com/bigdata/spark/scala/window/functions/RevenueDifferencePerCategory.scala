package com.bigdata.spark.scala.window.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max}

/**
  * Created by chas6003 on 24-04-2019.
  */
object RevenueDifferencePerCategory {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RevenueDifferencePerCategory").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val dataset = Seq(
      ("Thin",       "cell phone", 6000),
      ("Normal",     "tablet",     1500),
      ("Mini",       "tablet",     5500),
      ("Ultra thin", "cell phone", 5000),
      ("Very thin",  "cell phone", 6000),
      ("Big",        "tablet",     2500),
      ("Bendable",   "cell phone", 3000),
      ("Foldable",   "cell phone", 3000),
      ("Pro",        "tablet",     4500),
      ("Pro2",       "tablet",     6500)
    ).toDF("product", "category", "revenue")

    // Problem :: What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?

    val windowSpec = Window.partitionBy("category").orderBy('revenue.desc)
    val revenueDiff1 = max("revenue").over(windowSpec.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)) - 'revenue
    val revenueDiff2 = max("revenue").over(windowSpec.rangeBetween(Window.unboundedPreceding, Window.currentRow)) - 'revenue
    val revenueDiff3 = max("revenue").over(windowSpec.rangeBetween(Window.currentRow, Window.unboundedFollowing)) - 'revenue
    val revenueDiff4 = max("revenue").over(windowSpec.rangeBetween(1L, Window.currentRow)) - 'revenue
    val revenueDiff5 = max("revenue").over(windowSpec.rangeBetween(Window.currentRow, 1L)) - 'revenue

    val revenueDiff6 = max("revenue").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)) - 'revenue
    val revenueDiff7 = max("revenue").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) - 'revenue
    val revenueDiff8 = max("revenue").over(windowSpec.rowsBetween(Window.currentRow, Window.unboundedFollowing)) - 'revenue
    val revenueDiff9 = max("revenue").over(windowSpec.rowsBetween(1L, Window.currentRow)) - 'revenue
    val revenueDiff10 = max("revenue").over(windowSpec.rowsBetween(Window.currentRow, 1L)) - 'revenue

    val result = dataset
      .withColumn("revenue_diff1", revenueDiff1)
      .withColumn("revenue_diff6", revenueDiff6)
      .withColumn("revenue_diff2", revenueDiff2)
      .withColumn("revenue_diff7", revenueDiff7)
      .withColumn("revenue_diff3", revenueDiff3)
      .withColumn("revenue_diff8", revenueDiff8)
      .withColumn("revenue_diff4", revenueDiff4)
      .withColumn("revenue_diff9", revenueDiff9)
      .withColumn("revenue_diff5", revenueDiff5)
      .withColumn("revenue_diff10", revenueDiff10)
    result.show()
  }
}
