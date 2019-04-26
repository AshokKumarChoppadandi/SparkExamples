package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SumOfNumbersProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/prime_nums.text");
        JavaRDD<String> rdd2 = rdd1.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator());
        JavaRDD<String> rdd3 = rdd2.filter(str -> !(str.equals("") && str.length() == 0));
        JavaRDD<Integer> rdd4 = rdd3.map(num -> Integer.parseInt(num.trim()));

        List<Integer> result = rdd4.collect();
        result.forEach(x -> System.out.println(x));

        Integer sumOfNumbers = rdd4.reduce((num1, num2) -> num1 + num2);
        System.out.println("Sum of Numbers :: " + sumOfNumbers);
    }
}
