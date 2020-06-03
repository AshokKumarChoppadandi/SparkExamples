package com.bigdata.spark.sql

import org.apache.spark.sql.functions.{col, collect_list, datediff, explode, split, to_date, udf, when}
import org.apache.spark.sql.types.{DataTypes, DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object GenerateDynamicQuery2 {

  val STRING_PATTERN: String = "^[a-zA-Z ]*"
  val NUMBERS_PATTERN: String = "^[0-9]*"
  val DECIMAL_PATTERN: String = "^[0-9]*\\.[0-9]*"
  val DATE_PATTERN: String = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
  val DATE_FORMAT: String = "yyyy-MM-dd"

  def getDataType: (String, String) => String = (minValue: String, maxValue: String) => {
    (maxValue, minValue) match {
      case s if minValue.matches(NUMBERS_PATTERN) & maxValue.matches(NUMBERS_PATTERN) => "long"
      case s if minValue.matches(DECIMAL_PATTERN) & maxValue.matches(DECIMAL_PATTERN) => "double"
      case s if minValue.matches(DATE_PATTERN) & maxValue.matches(DATE_PATTERN) => "date"
      case _ => "string"
    }
  }

  def getSchema: StructType = {
    DataTypes.createStructType(
      Array(
        DataTypes.createStructField("database", StringType, true),
        DataTypes.createStructField("table", StringType, true),
        DataTypes.createStructField("column", StringType, true),
        DataTypes.createStructField("min_value", StringType, true),
        DataTypes.createStructField("max_value", StringType, true),
        DataTypes.createStructField("max_min_diff", StringType, true)
      )
    )
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      //.master("local")
      .appName("GenerateDynamicQuery")
      .enableHiveSupport()
      .getOrCreate()

    val getColumnDataType = udf(getDataType)

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    val inputColumnsFile = args(0)
    val outputFileLocation = args(1)

    val columns = sparkSession.read.option("header", value = true).csv(inputColumnsFile)

    val df1 = columns
      .groupBy(col("database"), col("table"))
      .agg(collect_list("column").as("column_list"))

    val rdd1 = df1.map(group => {
      val dbName = group.getAs[String](0)
      val tableName = group.getAs[String](1)
      val temp = group.getAs[Seq[String]](2)

      val minMaxSelectString = temp.map(columnName => {
        s"""
           | concat("$columnName", ",", min($columnName), ",", max($columnName))
        """.stripMargin
      }).mkString(", \":\", ")

      val query =
        s"""
           | select "$dbName" as dbName, "$tableName" as tableName, split(concat(${minMaxSelectString}), ':') as split_col from $dbName.$tableName
      """.stripMargin

      query
    })

    val diffCol1 = col("maxValue").cast(LongType).minus(col("minValue").cast(LongType))
    val diffCol2 = col("maxValue").cast(DoubleType).minus(col("minValue").cast(LongType))
    val diffCol3 = datediff(to_date(col("maxValue"), DATE_FORMAT), to_date(col("minValue"), DATE_FORMAT))

    val resultLists = rdd1.collect().map(query => {
      sparkSession
        .sql(query)
        .withColumn("exp_col", explode(col("split_col")))
        .select("dbName", "tableName", "exp_col")
        .withColumn("colName", split(col("exp_col"), ",")(0))
        .withColumn("minValue", split(col("exp_col"), ",")(1))
        .withColumn("maxValue", split(col("exp_col"), ",")(2))
        .withColumn("col_data_type", getColumnDataType(col("minValue"), col("maxValue")))
        .withColumn("diff",
          when(col("col_data_type") === "long", diffCol1.cast(StringType))
            .when(col("col_data_type") === "double", diffCol2.cast(StringType))
            .when(col("col_data_type") === "date", diffCol3.cast(StringType))
            .otherwise("NULL"))
        .select("dbName", "tableName", "colName", "minValue", "maxValue", "diff")
        .collect()
    })

    val results = resultLists.flatten
    val finalDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(results), getSchema)
    finalDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", value = true).csv(outputFileLocation)
  }
}
