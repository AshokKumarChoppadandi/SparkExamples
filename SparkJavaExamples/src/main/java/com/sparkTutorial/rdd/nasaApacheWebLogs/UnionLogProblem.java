package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("UnionLogProblem");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddJuly1 = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> rddAugust1 = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> unionRdd = rddJuly1.union(rddAugust1);
        //JavaRDD<String> filteredRdd = unionRdd.filter(line -> !(line.startsWith("host") && line.contains("bytes")));
        JavaRDD<String> filteredRdd = unionRdd.filter(line -> isNotHeader(line));

        JavaRDD<String> sampleRdd = filteredRdd.sample(false, 0.1);
        List<String> result = sampleRdd.collect();

        result.forEach(x -> System.out.println(x));
    }

    private static Boolean isNotHeader(String str) {
        return !(str.startsWith("host") && str.contains("bytes"));
    }
}
