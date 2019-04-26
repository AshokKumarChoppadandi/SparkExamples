package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsByCountryProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

        SparkConf conf = new SparkConf().setAppName("AirportsByCountryProblem").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/airports.text");
        JavaRDD<String[]> rdd2 = rdd1.map(line -> line.split(Utils.COMMA_DELIMITER));
        JavaPairRDD<String, String> rdd3 = rdd2.mapToPair(arr -> new Tuple2<>(arr[3], arr[1]));
        JavaPairRDD<String, Iterable<String>> rdd4 = rdd3.groupByKey();
        rdd4.collect().forEach(x -> System.out.println(x._1 + " : " + x._2));
    }
}
