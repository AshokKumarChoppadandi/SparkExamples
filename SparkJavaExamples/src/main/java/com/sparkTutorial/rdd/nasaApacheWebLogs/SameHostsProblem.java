package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SameHostsProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddJuly1 = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> rddAugust1 = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> julyHosts = rddJuly1.map(line -> line.split("\t")[0]);
        JavaRDD<String> augustHosts = rddAugust1.map(line -> line.split("\t")[0]);

        JavaRDD<String> intersectRDD = julyHosts.intersection(augustHosts);
        JavaRDD<String> sameHosts = intersectRDD.filter(line -> !line.startsWith("host"));

        List<String> result = sameHosts.collect();
        result.forEach(x -> System.out.println(x));
    }
}
