package com.bigdata.spark.jdbc

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.{DriverManager, PreparedStatement}
import java.util.Properties

object MySQLUpsertsWithSpark {
  def main(args: Array[String]): Unit = {

    val list = List(
      (107, "g@gmail.com", "G", 2000, 995),
      (105, "e@gmail.com", "E", 5000, 111),
      (104, "d@gmail.com", "D", 10000, 111)
    )

    val spark = SparkSession.builder().appName("MySQLUpsertsWithSpark").master("local").getOrCreate()

    val driver = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val password = "Password@123"

    val properties = new Properties()

    properties.put("user", userName)
    properties.put("password", password)
    properties.put("jdbcUrl", jdbcUrl)
    properties.put("jdbcDriver", driver)

    val bcVar = spark.sparkContext.broadcast(properties)

    import spark.implicits._
    val df1 = list.toDF("eid", "email", "ename", "salary", "edept_id")

    df1.repartition(3).foreachPartition(partition => {

      val connectionProperties = bcVar.value

      // Extracting values from broadcast variable
      val bcUsername = connectionProperties.getProperty("user")
      val bcPassword = connectionProperties.getProperty("password")
      val bcJdbcUrl = connectionProperties.getProperty("jdbcUrl")
      val bcJdbcDriver = connectionProperties.getProperty("jdbcDriver")

      Class.forName(bcJdbcDriver)

      val dbConnection = DriverManager.getConnection(bcJdbcUrl, bcUsername, bcPassword)
      val dbBatchSize = 1

      partition.grouped(dbBatchSize).foreach(batch => {

        val upsertQuery =
          """
            | INSERT INTO db1.employees (eid, email, ename, salary, edept_id)
            | VALUES (?, ?, ?, ?, ?)
            | ON DUPLICATE KEY UPDATE ename = ?,salary = ?, edept_id = ?
          """.stripMargin

        var stmt: PreparedStatement = null
          batch.foreach(dfRow => {
          val eIdIndex = dfRow.fieldIndex("eid")
          val eId = dfRow.getInt(eIdIndex)

          val emailIndex = dfRow.fieldIndex("email")
          val email = dfRow.getString(emailIndex)

          val eNameIndex = dfRow.fieldIndex("ename")
          val eName = dfRow.getString(eNameIndex)

          val eSalaryIndex = dfRow.fieldIndex("salary")
          val eSalary = dfRow.getInt(eSalaryIndex)

          val eDeptIdIndex = dfRow.fieldIndex("edept_id")
          val eDeptId = dfRow.getInt(eDeptIdIndex)

          stmt = dbConnection.prepareStatement(upsertQuery)
          stmt.setInt(1, eId)
          stmt.setString(2, email)
          stmt.setString(3, eName)
          stmt.setInt(4, eSalary)
          stmt.setInt(5, eDeptId)
          stmt.setString(6, eName)
          stmt.setInt(7, eSalary)
          stmt.setInt(8, eDeptId)

          stmt.addBatch()
        })

        stmt.executeBatch()
        // dbConnection.commit()
        stmt.close()
      })

      dbConnection.close()
    })
  }
}
