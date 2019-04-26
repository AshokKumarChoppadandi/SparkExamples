package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Iterator;

public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("AverageHousePriceProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/RealEstate.csv").filter(line -> !line.startsWith("MLS"));
        JavaRDD<String[]> rdd2 = rdd1.map(line -> line.split(","));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd3 = rdd2.mapToPair(arr -> new Tuple2<>(Integer.valueOf(arr[3]), new Tuple2<>(Double.valueOf(arr[2]), 1)));

        // Using Reduce By Key
        JavaPairRDD<Integer, Tuple2<Double, Integer>> rdd4 = rdd3.reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));
        JavaPairRDD<Integer, Double> rdd5 = rdd4.mapValues(tuple -> tuple._1() / tuple._2());
        rdd5.collect().forEach(x -> System.out.println(x));

        // Using Group By Key
        JavaPairRDD<Integer, Double> rdd6 = rdd2.mapToPair(arr -> new Tuple2<>(Integer.valueOf(arr[3]), Double.valueOf(arr[2])));
        JavaPairRDD<Integer, Iterable<Double>> rdd7 = rdd6.groupByKey();
        JavaPairRDD<Integer, Double> result = rdd7.mapValues(x -> {
            int numOfHomes = Iterables.size(x);
            Double totalPrice = 0.0;
            Iterator<Double> prices = x.iterator();
            while(prices.hasNext()) {
                totalPrice += prices.next();
            }

            return totalPrice / numOfHomes;
        });

        result.collect().forEach(x -> System.out.println(x._1() + " : " + x._2()));
    }

}
