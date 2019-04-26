package com.sparkTutorial.rdd.reduce;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ReduceExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);
        /*
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        Integer product = integerRdd.reduce((x, y) -> x * y);

        System.out.println("product is :" + product);
        */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd1 = sc.parallelize(ints, 3);

        JavaRDD<Integer> rdd2 = rdd1.map(num -> num * num);
        Integer result = rdd2.reduce((num1, num2) -> num1 + num2);

        System.out.println("Sum of Squares of Numbers is :: " + result);
    }
}
