package com.sparkTutorial.pairRdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromTupleList {

    public static void main(String[] args) throws Exception {

        /*
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> tuple = Arrays.asList(new Tuple2<>("Lily", 23),
                                                            new Tuple2<>("Jack", 29),
                                                            new Tuple2<>("Mary", 29),
                                                            new Tuple2<>("James",8));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuple);

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list");
        */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRddFromTupleList");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> tuples = Arrays.asList(
                new Tuple2<>(1, "Hadoop"),
                new Tuple2<>(2, "Spark"),
                new Tuple2<>(3, "Hive"),
                new Tuple2<>(4, "Kafka"),
                new Tuple2<>(5, "Docker")
        );

        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(tuples, 3);

        List<Tuple2<Integer, String>> result = rdd.collect();
        result.forEach(x -> System.out.println(x._1 + " -> " + x._2));
    }
}
