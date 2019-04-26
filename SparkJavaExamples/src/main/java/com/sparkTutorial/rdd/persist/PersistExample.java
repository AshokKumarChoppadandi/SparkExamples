package com.sparkTutorial.rdd.persist;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.scalactic.Bool;

import java.util.Arrays;
import java.util.List;

public class PersistExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        /*
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        integerRdd.persist(StorageLevel.MEMORY_ONLY());

        integerRdd.reduce((x, y) -> x * y);

        integerRdd.count();
        */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("PersistExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd1 = sc.parallelize(ints, 3);

        rdd1.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<Integer> rdd2 = rdd1.map(num -> num * num);
        //JavaRDD<Integer> rdd3 = rdd2.filter(num -> num % 2 == 0);
        JavaRDD<Integer> rdd3 = rdd2.filter(num -> isEvenNumber(num));

        List<Integer> result = rdd3.collect();
        result.forEach(x -> System.out.println(x));
    }

    private static Boolean isEvenNumber(Integer i) {
        return i % 2 == 0;
    }
}
