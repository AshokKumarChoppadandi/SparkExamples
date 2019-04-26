package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */


        SparkConf conf = new SparkConf().setAppName("AirportsByLatitudeProblem").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/airports.text");
        JavaRDD<String[]> rdd2 = rdd1.map(line -> line.split(Utils.COMMA_DELIMITER));
        JavaRDD<Tuple2<String, Double>> rdd3 = rdd2.map(arr -> new Tuple2<>(arr[1], Double.parseDouble(arr[6])));
        JavaRDD<Tuple2<String, Double>> rdd4 = rdd3.filter(tuple -> tuple._2 > 40);

        List<Tuple2<String, Double>> result = rdd4.collect();

        result.forEach(x -> {
            String airportCity = x._1;
            Double latitude = x._2;

            System.out.println("Airport " + airportCity + " is located at latitude of " + latitude + " degrees");
        });
    }
}
