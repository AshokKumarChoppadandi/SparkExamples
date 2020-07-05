package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object GenerateDynamicQueryBasedOnGivenInputColumns {
  def main(args: Array[String]): Unit = {

    val inputColumnsFilePath = args(0)

    val spark = SparkSession
      .builder()
      .appName("GenerateDynamicQueryBasedOnGivenInputColumns")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val requiredColumns = List("id", "name", "test")
    val dbName = "test_db"
    val tableName = "employee_text"

    // Using Spark SQL
    val tableData1 = spark.sql(s"select * from $dbName.$tableName")

    val tempTableName = "temp_table"
    tableData1.createOrReplaceTempView(tempTableName)
    val tableColumns1 = tableData1.columns
    val finalColumns1 = requiredColumns
      .map(column => (column, tableColumns1.contains(column)))
      .map{case(column, bool) => if(bool) s"$column as $column" else s"-1 as $column"}

    val finalSelectQuery1 = s"select ${finalColumns1.mkString(",")} from $tempTableName"
    println("Final Query :: " + finalSelectQuery1)

    val df1 = spark.sql(finalSelectQuery1)
    df1.show

    // Using Data Frames API
    val tableData2 = spark.read.table(s"$dbName.$tableName")
    val tableColumns2 = tableData2.columns
    val finalColumns2 = requiredColumns
      .map(column => (column, tableColumns2.contains(column)))
      .map{case(column, bool) => if(bool) col(column) else lit(-1).as(s"$column")}

    val df2 = tableData2.select(finalColumns2: _*)
    df2.show()
  }
}
