package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("AirportsNotInUsaProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/airports.text");
        JavaRDD<String[]> rdd2 = rdd1.map(line -> line.split(Utils.COMMA_DELIMITER));
        JavaPairRDD<String, String> rdd3 = rdd2.mapToPair(arr -> new Tuple2<>(arr[1], arr[3]));
        JavaPairRDD<String, String> rdd4 = rdd3.filter(tuple -> !tuple._2.equals("\"United States\""));
        rdd4.collect().forEach(x -> System.out.println(x));
    }
}
