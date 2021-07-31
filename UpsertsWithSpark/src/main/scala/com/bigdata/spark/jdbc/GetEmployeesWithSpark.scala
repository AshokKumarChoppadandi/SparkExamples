package com.bigdata.spark.jdbc

import org.apache.spark.sql.SparkSession

import java.util.Properties

object GetEmployeesWithSpark {
  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val password = "Password@123"

    val spark = SparkSession
      .builder()
      .appName("MySQLUpsertsWithSpark")
      .master("local")
      .getOrCreate()

    val properties = new Properties()

    properties.put("user", userName)
    properties.put("password", password)
    properties.put("jdbcUrl", jdbcUrl)
    properties.put("jdbcDriver", driver)

    val readQuery = "(select * from db1.employees) tmp"
    val employeesDf = spark.read.jdbc(url = jdbcUrl, table = readQuery, properties = properties)

    employeesDf.printSchema()
    employeesDf.show()
  }
}
