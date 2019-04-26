package com.sparkTutorial.pairRdd.aggregation.combinebykey;

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePriceSolution {

    /*
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<String, Double> housePricePairRdd = cleanedLines.mapToPair(
                 line -> new Tuple2<>(line.split(",")[3],
                                      Double.parseDouble(line.split(",")[2])));

        JavaPairRDD<String, AvgCount> housePriceTotal= housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners);

        JavaPairRDD<String, Double> housePriceAvg = housePriceTotal.mapValues(avgCount -> avgCount.getTotal()/avgCount.getCount());

        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
        }
    }

    static Function<Double, AvgCount> createCombiner = x -> new AvgCount(1, x);

    static Function2<AvgCount, Double, AvgCount> mergeValue = (avgCount, x) -> new AvgCount(avgCount.getCount() + 1,
                                                                                             avgCount.getTotal() + x);
    static Function2<AvgCount, AvgCount, AvgCount> mergeCombiners =
            (avgCountA, avgCountB) -> new AvgCount(avgCountA.getCount() + avgCountB.getCount(),
                                                   avgCountA.getTotal() + avgCountB.getTotal());
    */

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AverageHousePriceSolution");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> rdd2 = rdd1.filter(line -> !(line.startsWith("MLS") || line.contains("Bedrooms")));
        JavaRDD<String[]> rdd3 = rdd2.map(line -> line.split(","));
        JavaPairRDD<Integer, Double> rdd4 = rdd3.mapToPair(arr ->  new Tuple2<>(Integer.valueOf(arr[3]), Double.valueOf(arr[2])));

        // 1. Using Lambda Functions
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd5 = rdd4.combineByKey(
          x -> new Tuple2<>(x, 1),
                (x, y) -> new Tuple2<>(x._1() + y, x._2() + 1),
                (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2())
        );

        JavaPairRDD<Integer, Double> result = rdd5.mapValues(tuple -> tuple._1() / tuple._2());
        result.collect().forEach(x -> System.out.println(x));

        // Using Anonymous Functions
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd6 = rdd4.combineByKey(
                createCombiner2,
                mergeValue2,
                mergeCombiner2
        );
        JavaPairRDD<Integer, Double> result2 = rdd6.mapValues(tuple -> tuple._1() / tuple._2());
        result2.collect().forEach(x -> System.out.println(x));

        // Using Lambda Functions
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd7 = rdd4.aggregateByKey(
                new Tuple2<>(0.0, 0),
                (x, y) -> new Tuple2<>(x._1() + y, x._2() + 1),
                (x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)
        );
        JavaPairRDD<Integer, Double> result3 = rdd7.mapValues(tuple -> tuple._1() / tuple._2());
        result3.collect().forEach(x -> System.out.println(x));

        // Using Anonymous Functions
        Tuple2<Double, Integer> initValue = new Tuple2<>(0.0, 0);
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd8 = rdd4.aggregateByKey(
                initValue,
                mergeValue2,
                mergeCombiner2
        );
        JavaPairRDD<Integer, Double> result4 = rdd8.mapValues(tuple -> tuple._1() / tuple._2());
        result4.collect().forEach(x -> System.out.println(x));
    }

    private static Function<Double, Tuple2<Double, Integer>> createCombiner2 = x -> new Tuple2<>(x, 1);
    private static Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>> mergeValue2 = (x, y) -> new Tuple2<>(x._1() + y, x._2() + 1);
    private static Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> mergeCombiner2 = (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2());
}
