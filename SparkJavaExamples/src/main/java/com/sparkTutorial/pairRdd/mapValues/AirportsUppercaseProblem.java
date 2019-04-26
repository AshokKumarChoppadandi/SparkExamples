package com.sparkTutorial.pairRdd.mapValues;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class AirportsUppercaseProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("AirportsUppercaseProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(getPairElements());
        JavaPairRDD<String, String> rdd3 = rdd2.mapValues(country -> country.toUpperCase());

        List<Tuple2<String, String>> result = rdd3.collect();
        result.forEach(x -> System.out.println(x));
    }

    private static PairFunction<String, String, String> getPairElements() {
        return (PairFunction<String, String, String>) str -> new Tuple2<>(
                str.split(Utils.COMMA_DELIMITER)[1],
                str.split(Utils.COMMA_DELIMITER)[3]
        );
    }
}
