package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("AirportsInUsaProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/airports.text");
        JavaRDD<String[]> rdd2 = rdd1.map(line -> line.split(Utils.COMMA_DELIMITER));

        JavaRDD<String[]> rdd3 = rdd2.filter(arr -> arr[3].equals("\"United States\""));
        JavaRDD<String> rdd4 = rdd3.map(arr -> StringUtils.join(new String[]{arr[1], arr[2]}, ","));

        List<String> result = rdd4.collect();
        result.forEach(x -> System.out.println(x));

    }
}
