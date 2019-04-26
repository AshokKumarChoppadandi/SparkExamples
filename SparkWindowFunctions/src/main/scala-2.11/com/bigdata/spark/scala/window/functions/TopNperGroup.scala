package com.bigdata.spark.scala.window.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank}

/**
  * Created by chas6003 on 24-04-2019.
  */
object TopNperGroup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNperGroup").master("local").getOrCreate()
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

    // Problem :: What are the best-selling and the second best-selling products in every category?

    val windowSpec = Window.partitionBy("category").orderBy($"revenue".desc)
    val denseRnkByRevenue = dense_rank().over(windowSpec)

    val result1 = dataset.select('*, denseRnkByRevenue as 'dense_rnk).filter($"dense_rnk".<=(2))
    result1.show()
  }
}
