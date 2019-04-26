package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
/**
 * Created by chas6003 on 26-04-2019.
 */
public class TypedDataset2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("TypedDataset2").master("local").getOrCreate();

        Dataset<Row> emp1 = spark.read().csv("in/employees").select(
                col("_c0").cast("integer").as("eid"),
                col("_c1").as("ename"),
                col("_c2").cast("integer").as("dno")
        );
        Dataset<Employee> emp2 = emp1.as(Encoders.bean(Employee.class));
        emp2.show();
        emp2.filter((FilterFunction<Employee>) rec -> rec.getDno() >= 102).show();
        emp2.filter(empFilter).show();

        Dataset<Employee> emp3 = emp2.map((MapFunction<Employee, Employee>) emp -> {
            Employee e = new Employee();
            int eid = emp.getEid() * 2;
            e.setEid(eid);
            e.setEname(emp.getEname());
            e.setDno(emp.getDno());
            return e;
        }, Encoders.bean(Employee.class));
        emp3.show();

        Dataset<Employee> emp4 = emp2.map(empMyMap, Encoders.bean(Employee.class));
        emp4.show();


    }

    private static FilterFunction<Employee> empFilter = rec -> rec.getDno() >= 102;

    private static MapFunction<Employee, Employee> empMyMap = rec -> new Employee(rec.getEid() * 2, rec.getEname(), rec.getDno());
}
