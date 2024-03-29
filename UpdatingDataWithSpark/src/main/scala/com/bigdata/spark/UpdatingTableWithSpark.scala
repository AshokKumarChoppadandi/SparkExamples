package com.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object UpdatingTableWithSpark {
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

    // 3. Only few Employee fields are modified / updated
    val partiallyUpdatedEmployeeData = Seq (
      PartiallyUpdatedEmployee(3, 12000.0),
      PartiallyUpdatedEmployee(5, 23000.0)
    )

    val partiallyUpdatedEmployeeDf = partiallyUpdatedEmployeeData.toDF()
    // val partiallyUpdatedEmployeeDf = spark.sql("select * from dbName.partially_updated_employees")

    partiallyUpdatedEmployeeDf.printSchema()
    partiallyUpdatedEmployeeDf.show()

    // Steps to Update the Employee (Original) table --- Full fields get changed
    // 1. Do Left Outer Join the Original Employee Table with the Updated Employee Table

    val joinedData1 = employeeDf.alias("emp").join(
      updatedEmployeeDf.alias("updatedEmp"),
      employeeDf.col("eid") === updatedEmployeeDf.col("eid"),
      "left_outer")

    joinedData1.printSchema()
    joinedData1.show()

    // 2. Fetch Not Updated Records
    val notUpdatedRecords = joinedData1.filter(col("updatedEmp.eid").isNull).select("emp.*")
    notUpdatedRecords.printSchema()
    notUpdatedRecords.show()

    // 3. Fetch the Updated Records
    val updatedRecords = joinedData1.filter(col("updatedEmp.eid").isNotNull).select("updatedEmp.*")
    updatedRecords.printSchema()
    updatedRecords.show()

    // 4. Union NotUpdated and Updated records to form the Full Employee table

    val updatedEmployees = notUpdatedRecords.union(updatedRecords)
    updatedEmployees.printSchema()
    updatedEmployees.show()

    // Steps to Update the Employee (Original) table --- Few fields get changed
    // 1. Do Left Outer Join the Original Employee Table with the Partially Updated Employee Table

    val joinedData2 = employeeDf.alias("emp").join(
      partiallyUpdatedEmployeeDf.alias("partUpdatedEmp"),
      employeeDf.col("eid") === partiallyUpdatedEmployeeDf.col("eid"),
      "left_outer")

    joinedData2.printSchema()
    joinedData2.show()

    // 2. Fetch Not Updated Records
    val notUpdatedRecords2 = joinedData2.filter(col("partUpdatedEmp.eid").isNull).select("emp.*")
    notUpdatedRecords2.printSchema()
    notUpdatedRecords2.show()

    // 3. Fetch the Updated Records
    val updatedRecords2 = joinedData2.filter(col("partUpdatedEmp.eid").isNotNull).select($"emp.eid", $"emp.ename", $"partUpdatedEmp.esalary"/*.as("esalary")*/, $"emp.edept")
    updatedRecords2.printSchema()
    updatedRecords2.show()

    // 4. Union NotUpdated and Updated records to form the Full Employee table
    val partiallyUpdatedEmployees = notUpdatedRecords2.union(updatedRecords2)
    partiallyUpdatedEmployees.printSchema()
    partiallyUpdatedEmployees.show()

    // NOTE: IF WE DEAL WITH PHYSICAL TABLE LIKE HIVE TABLES THEN THE BELOW STEPS ARE NEED TO PERFORM UPDATE
    // 5. Create and Insert All the Updated Employees Data into a Temporary Table

    // 6. Truncate the Original Table

    // 7. Insert the data from Temporary Table to the Original Table
  }
}
