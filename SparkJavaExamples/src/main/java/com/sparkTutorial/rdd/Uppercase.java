package com.sparkTutorial.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Uppercase {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        /*
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        JavaRDD<String> lowerCaseLines = lines.map(line -> line.toUpperCase());

        lowerCaseLines.saveAsTextFile("out/uppercase.text");
        */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("UpperCase");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/uppercase.text");
        JavaRDD<String> rdd2 = rdd1.map(line -> line.toUpperCase());

        List<String> result = rdd2.take(10);

        result.forEach(x -> System.out.println(x));
    }
}
