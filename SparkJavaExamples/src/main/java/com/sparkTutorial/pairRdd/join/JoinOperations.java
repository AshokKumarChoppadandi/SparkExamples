package com.sparkTutorial.pairRdd.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinOperations {

    public static void main(String[] args) throws Exception {
        /*
        SparkConf conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> ages = sc.parallelizePairs(Arrays.asList(new Tuple2<>("Tom", 29),
                                                                              new Tuple2<>("John", 22)));

        JavaPairRDD<String, String> addresses = sc.parallelizePairs(Arrays.asList(new Tuple2<>("James", "USA"),
                                                                                  new Tuple2<>("John", "UK")));

        JavaPairRDD<String, Tuple2<Integer, String>> join = ages.join(addresses);

        join.saveAsTextFile("out/age_address_join.text");

        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftOuterJoin = ages.leftOuterJoin(addresses);

        leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.text");

        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightOuterJoin = ages.rightOuterJoin(addresses);

        rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.text");

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = ages.fullOuterJoin(addresses);

        fullOuterJoin.saveAsTextFile("out/age_address_full_out_join.text");
        */

        SparkConf conf = new SparkConf().setMaster("local").setAppName("JoinOperations");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list1 = Arrays.asList(
                "bigdata ashok", "admin naveen"
        );

        List<String> list2 = Arrays.asList(
                "bigdata spark", "database cassandra"
        );

        JavaRDD<String> rdd11 = sc.parallelize(list1, 2);
        JavaRDD<String> rdd21 = sc.parallelize(list2, 2);

        JavaPairRDD<String, String> rddPair1 = rdd11.mapToPair(getPairElements());
        JavaPairRDD<String, String> rddPair2 = rdd21.mapToPair(getPairElements());

        JavaPairRDD<String, Tuple2<String, String>> innerJoinRdd = rddPair1.join(rddPair2);
        innerJoinRdd.collect().forEach(x -> System.out.println(x));

        System.out.println("-------------------------------------------------");

        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinRdd = rddPair1.leftOuterJoin(rddPair2);
        leftOuterJoinRdd.collect().forEach(x -> System.out.println(x));

        System.out.println("-------------------------------------------------");
        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightOuterJoinRdd = rddPair1.rightOuterJoin(rddPair2);
        rightOuterJoinRdd.collect().forEach(x -> System.out.println(x));

        System.out.println("-------------------------------------------------");
        JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> fullOuterJoinRdd = rddPair1.fullOuterJoin(rddPair2);
        fullOuterJoinRdd.collect().forEach(x -> System.out.println(x));

        System.out.println("-------------------------------------------------");

    }

    private static PairFunction<String, String, String> getPairElements() {
        return (PairFunction<String, String, String>) str -> new Tuple2<>(
                str.split(" ")[0],
                str.split(" ")[1]
        );
    }
}
