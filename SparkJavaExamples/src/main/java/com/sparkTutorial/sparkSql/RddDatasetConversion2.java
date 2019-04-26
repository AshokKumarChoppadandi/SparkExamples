package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chas6003 on 26-04-2019.
 */
public class RddDatasetConversion2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("RddDatasetConversion2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("RddDatasetConversion2").master("local").getOrCreate();

        List<String> employees = Arrays.asList(
                "1,Ashok,101",
                "2,Kumar,102",
                "3,Choppadandi,103"
        );
        List<String> departments = Arrays.asList(
                "101,IT,Information Technology",
                "102,ADMIN, Administration",
                "103,HR,Human Resources"
        );

        JavaRDD<String> empRdd1 = sc.parallelize(employees, 2);
        JavaRDD<String> depRdd1 = sc.parallelize(departments, 2);

        JavaRDD<Employee> employeeJavaRDD = empRdd1.map(x -> {
            String[] arr = x.split(",");
            return new Employee(Integer.valueOf(arr[0]), arr[1], Integer.valueOf(arr[2]));
        });

        JavaRDD<Department> departmentJavaRDD = depRdd1.map(x -> {
           String[] arr = x.split(",");
           return new Department(Integer.valueOf(arr[0]), arr[1], arr[2]);
        });

        employeeJavaRDD.collect().forEach(System.out::println);
        departmentJavaRDD.collect().forEach(System.out::println);

        Dataset<Employee> employeeDataset = spark.createDataset(employeeJavaRDD.rdd(), Encoders.bean(Employee.class));
        employeeDataset.show();
        Dataset<Row> rowDataset = spark.createDataFrame(employeeJavaRDD, Employee.class);
        rowDataset.show();

    }
}
