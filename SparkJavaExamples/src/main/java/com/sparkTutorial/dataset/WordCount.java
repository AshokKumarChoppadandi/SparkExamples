package com.sparkTutorial.dataset;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("WordCount").master("local").getOrCreate();

        List<String> list = Arrays.asList(
                "Hello World Hello Spark Hello Java",
                "World is wonderful",
                "Java is a Programming Language"
        );

        Dataset<String> dataset1 = sparkSession.createDataset(list, Encoders.STRING());
        Dataset<String> dataset2 = dataset1.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split("\\W+")).iterator(), Encoders.STRING());

        RelationalGroupedDataset dataset3 = dataset2.groupBy("value");
        Dataset<Row> dataset4 = dataset3.count();
        dataset4.show();
    }
}
