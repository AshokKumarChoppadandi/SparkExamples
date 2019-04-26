package com.sparkTutorial.pairRdd.sort;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SortedWordCountProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/word_count.text");
        JavaRDD<String> rdd2 = rdd1.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator());

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(num -> new Tuple2<>(num, 1));
        JavaPairRDD<String, Integer> wordCounts = rdd3.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> wordCountsReverse = wordCounts.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
        JavaPairRDD<Integer, String> sortedWordCountsReverse = wordCountsReverse.sortByKey(false);

        JavaPairRDD<String, Integer> sortedWordCounts = sortedWordCountsReverse.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
        List<Tuple2<String, Integer>> result = sortedWordCounts.collect();

        for(Tuple2<String, Integer> tuple : result) {
            System.out.println(tuple._1() + " : " + tuple._2());
        }

    }

}

