package com.sparkTutorial.pairRdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromRegularRdd {

    /*
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");

        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);

        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(getPairFunction());

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
    }

    private static PairFunction<String, String, Integer> getPairFunction() {
        return s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
    }
    */

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRddFromRegularRdd");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputStrings = Arrays.asList("1 hadoop", "2 spark", "3 hive", "4 kafka", "5 docker");
        JavaRDD<String> rdd1 = sc.parallelize(inputStrings, 3);

        JavaPairRDD tuples = rdd1.mapToPair(getPairFunction());
        List result = tuples.collect();
        result.forEach(x -> System.out.println(x));
    }

    private static PairFunction<String, Integer, String> getPairFunction() {
        return str -> new Tuple2<>(Integer.valueOf(str.split(" ")[0]), str.split(" ")[1]);
    }
}
