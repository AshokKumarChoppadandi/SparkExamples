package com.bigdata.spark.jdbc

import java.sql.DriverManager

object GetEmployees {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://localhost:3306/db1"
    val userName = "root"
    val password = "Password@123"

    Class.forName(driver)
    val connection = DriverManager.getConnection(jdbcUrl, userName, password)
    val statement = connection.createStatement()

    val resultSet = statement.executeQuery("select * from db1.employees")
    while(resultSet.next()) {
      val eId = resultSet.getInt("eid")
      val email = resultSet.getString("email")
      val eName = resultSet.getString("ename")
      val eSalary = resultSet.getInt("salary")
      val eDept = resultSet.getInt("edept_id")

      val resultString =
        s"""
           | eId = $eId
           | email = $email
           | eName = $eName
           | eSalary = $eSalary
           | eDept = $eDept
        """.stripMargin

      println(resultString)
    }
  }
}
