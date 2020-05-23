package com.sparkTutorial.dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class FilterAndAdd {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("FilterAndAdd").master("local").getOrCreate();

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        Dataset<Integer> dataset1 = sparkSession.createDataset(numbers, Encoders.INT());
        Dataset<Integer> dataset2 = dataset1.filter((FilterFunction<Integer>) x -> x % 2 == 0);
        Dataset<Integer> dataset3 = dataset2.map((MapFunction<Integer, Integer>) x -> x * x, Encoders.INT());

        Integer result = dataset3.reduce((ReduceFunction<Integer>) Integer::sum);
        System.out.println("Sum of Squares of Even Numbers between 1 and 10 :: " + result);
    }
}
