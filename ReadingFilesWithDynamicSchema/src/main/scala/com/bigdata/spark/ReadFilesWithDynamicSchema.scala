package com.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by AshokKumarChoppadandi on 27-06-2019
  */
object ReadFilesWithDynamicSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadFilesWithDynamicSchema").master("local").getOrCreate()
    val arg1 = args(0)
    val arg2 = args(1)

    val headers = spark.read.textFile(s"${arg1}${arg2}").head().split(",")
    val buffer = new ArrayBuffer[StructField]()
    headers.foreach(
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

    val schema = new StructType(buffer.toArray)

    val df = spark.read.schema(schema).csv(s"${arg1}*.csv")
    df.printSchema()
    df.show()
  }
}
