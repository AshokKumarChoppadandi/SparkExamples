package com.bigdata.spark.jdbc

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object InsertEmployees {
  def main(args: Array[String]): Unit = {
    val list = List(
      (106, "f@gmail.com", "f", 2000, 995)
    )

    val driver = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val password = "Password@123"

    val properties = new Properties()

    properties.put("user", userName)
    properties.put("password", password)
    properties.put("jdbcUrl", jdbcUrl)
    properties.put("jdbcDriver", driver)

    Class.forName(driver)

    val spark = SparkSession.builder().appName("MySQLUpsertsWithSpark").master("local").getOrCreate()

    import spark.implicits._

    val df1 = list.toDF("eid", "email", "ename", "salary", "edept_id")

    df1.printSchema()
    df1.show()

    df1.write.mode(SaveMode.Append).jdbc(url = jdbcUrl, table = "db1.employees", connectionProperties = properties)
  }
}
