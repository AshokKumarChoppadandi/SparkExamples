package com.sparkTutorial.advanced.accumulator;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

public class StackOverFlowSurvey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        /*
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);

        JavaRDD<String> responseRDD = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {
            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);

            total.add(1);

            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            return splits[2].equals("Canada");

        });

        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
        */

        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local");
        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();

        total.register(sparkContext, Option.apply("total count"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary midpoint"), false);

        JavaRDD<String> rdd1 = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");
        JavaRDD<String> rdd2 = rdd1.filter(line -> {

            String[] arr = line.split(Utils.COMMA_DELIMITER, -1);
            total.add(1);

            if(arr[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            return arr[2].equals("Canada");
        });

        System.out.println("Total responses from Canada :: " + rdd2.count());
        System.out.println("Total Number of records :: " + total.value());
        System.out.println("Total Number of salary missing records :: " + missingSalaryMidPoint.value());
    }
}
