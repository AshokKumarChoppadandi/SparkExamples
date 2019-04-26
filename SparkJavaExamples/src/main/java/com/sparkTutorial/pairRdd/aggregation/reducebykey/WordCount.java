package com.sparkTutorial.pairRdd.aggregation.reducebykey;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        /*
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        Map<String, Integer> worldCountsMap = wordCounts.collectAsMap();

        for (Map.Entry<String, Integer> wordCountPair : worldCountsMap.entrySet()) {
            System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());

        }
        */

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/word_count.text");
        JavaRDD<String> rdd2 = rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> result = rdd4.collect();
        for(Tuple2<String, Integer> tuple : result) {
            System.out.println(tuple._1() + " : " + tuple._2());
        }
    }
}
