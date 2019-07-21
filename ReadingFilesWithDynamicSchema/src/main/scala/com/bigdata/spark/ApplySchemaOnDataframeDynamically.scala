package com.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ApplySchemaOnDataframeDynamically {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ApplySchemaOnDataframeDynamically").master("local").getOrCreate()
    // Input table Schema
    val inputTableSchema = new StructType(
      Array(
        StructField("id", StringType, true),
        StructField("name", StringType, true),
        StructField("salary", StringType, true),
        StructField("department", StringType, true)
      )
    )

    // Output Table Schema
    // 1. Creating the Schema - Static
    val outputTableSchema1 = new StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("salary", DoubleType, true),
        StructField("department", IntegerType, true)
      )
    )

    // 2. Creating the Schema - From File
    val lines = Source.fromFile("src/main/resources/test1/schema.txt").mkString
    val columnsWithTypes = lines.split(",");

    val buffer = new ArrayBuffer[StructField]()
    columnsWithTypes.foreach(
      column => {
        val colNameAndType = column.split(":")
        val colType = colNameAndType(1) match {
          case "Int" => IntegerType
          case "String" => StringType
          case "Double" => DoubleType
          case "Long" => LongType
          case "Float" => FloatType
          case "Date" => DateType
          case _ => StringType
        }
        buffer += new StructField(colNameAndType(0), colType, true)
      }
    )

    val outputTableSchema2 = new StructType(buffer.toArray)

    // 3. Creating the Schema - From a Physical Table
    // Commenting this because I don't have a table to read now.
    /*
    val dbName = "database"
    val tableName = "table"

    val emptyDf = spark.sql(s"select * from ${dbName}.${tableName} where 1 = 2");
    val ouptutTableSchema3 = emptyDf.schema;
    */

    // Creating the List of Columns and it's Types from the Output Schema
    // This case I'm using the 'outputTableSchema1', we can use other Schemas also
    val columnsWithDataTypes = new ArrayBuffer[(String, String)]()
    outputTableSchema1.fields.foreach(x => {
      val columnName = x.name
      val columnType = x.dataType.typeName
      val colNameAndType = (columnName, columnType)
      columnsWithDataTypes += colNameAndType
    })
    val finalColumnsWithTypes = columnsWithDataTypes.toArray

    // Reading Input Data - All the fields are String by Default
    val df = spark.read.schema(inputTableSchema).csv("src/main/resources/test1/employees.csv")
    df.printSchema()
    df.show()

    // Converting the Input Dataframe to the Output Dataframe by Type Casting the Data Types for the Columns
    val targetDf = df.select(finalColumnsWithTypes.map{case(colName, colType) => col(colName).cast(colType)}: _*)
    targetDf.printSchema()
    targetDf.show()

  }
}
