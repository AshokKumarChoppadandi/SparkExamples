package com.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object UpdatingFullTableUsingSparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("UpdatingTableWithSpark").getOrCreate()
    import spark.implicits._

    // 1. Original Employees Data
    val employeeData = Seq (
      Employee(1, "Alice", 20000.0, 123),
      Employee(2, "Bob", 15000.0, 111),
      Employee(3, "Charlie", 10000.0, 123),
      Employee(4, "David", 25000.0, 100),
      Employee(5, "Edward", 20000.0, 100)
    )

    val employeeDf = employeeData.toDF()
    // val employeeDf = spark.sql("select * from dbName.employees")

    employeeDf.printSchema()
    employeeDf.show()

    // 2. All fields except EID are modified / updated
    val updatedEmployeeData = Seq(
      UpdatedEmployee(1, "Alice_1", 25000.0, 999),
      UpdatedEmployee(2, "Bob_2", 35000.0, 789),
      UpdatedEmployee(4, "David_4", 40000.0, 990)
    )

    val updatedEmployeeDf = updatedEmployeeData.toDF()
    // val updatedEmployeeDf = spark.sql("select * from dbName.updated_employees")
    updatedEmployeeDf.printSchema()
    updatedEmployeeDf.show()

    employeeDf.createOrReplaceTempView("employees")
    updatedEmployeeDf.createOrReplaceTempView("updated_employees")

    // Steps to Update the Employee (Original) table --- Full fields get changed
    // 1. Do Left Outer Join the Original Employee Table with the Updated Employee Table

    val joinedData1 = spark.sql(
      """
        | SELECT emp.*, updatedEmp.eid as u_eid, updatedEmp.ename as u_ename, updatedEmp.esalary as u_esalary, updatedEmp.edept as u_edept
        | FROM employees emp LEFT OUTER JOIN updated_employees updatedEmp
        | ON (emp.eid = updatedEmp.eid)
      """.stripMargin)

    joinedData1.createOrReplaceTempView("joined_table")
    joinedData1.printSchema()
    joinedData1.show()

    // 2. Fetch Not Updated Records
    val notUpdatedRecords = spark.sql(
      """
        | SELECT eid, ename, esalary, edept
        | FROM joined_table
        | WHERE u_eid IS NULL
      """.stripMargin)
    notUpdatedRecords.printSchema()
    notUpdatedRecords.show()

    // 3. Fetch the Updated Records
    val updatedRecords = spark.sql(
      """
        | SELECT eid, u_ename as ename, u_esalary as esalary, u_edept as edept
        | FROM joined_table
        | WHERE u_eid IS NOT NULL
      """.stripMargin)
    updatedRecords.printSchema()
    updatedRecords.show()

    // 4. Union NotUpdated and Updated records to form the Full Employee table

    val updatedEmployees = notUpdatedRecords.union(updatedRecords)
    updatedEmployees.printSchema()
    updatedEmployees.show()

    // NOTE: IF WE DEAL WITH PHYSICAL TABLE LIKE HIVE TABLES THEN THE BELOW STEPS ARE NEED TO PERFORM UPDATE
    // 5. Create and Insert All the Updated Employees Data into a Temporary Table

    // 6. Truncate the Original Table

    // 7. Insert the data from Temporary Table to the Original Table
  }
}
