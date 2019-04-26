package com.sparkTutorial.advanced.broadcast;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by chas6003 on 25-04-2019.
 */
public class MyBroadcastExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        /*
        * 1,101
        * 2,202
        * 3,303
        * 4,404
        * 5,505
        *
        *
        * 101,IT
        * 202,CSE
        * 303,MECH
        * */

        List<String> students = Arrays.asList("1,101", "2,202", "3,303", "4,404", "5,505");
        List<String> departments = Arrays.asList("101,IT", "202,CSE", "303,MECH");

        SparkConf conf = new SparkConf().setAppName("MyBroadcastExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> studentsRdd = sc.parallelize(students, 2);
        JavaRDD<String> departmentsRdd = sc.parallelize(departments, 2);

        Map<String, String> deps = departmentsRdd.mapToPair(x -> new Tuple2<>(x.split(",")[0], x.split(",")[1])).collectAsMap();

        final Broadcast<Map<String, String>> bcVar = sc.broadcast(deps);

        // Left Outer Join
        JavaPairRDD<String, String> studentsPair = studentsRdd.mapToPair(
                x -> {
                    String[] arr = x.split(",");
                    Set<String> keys = bcVar.value().keySet();
                    if(keys.contains(arr[1])) {
                        String depName = bcVar.value().get(arr[1]);
                        return new Tuple2<>(arr[0], depName);
                    } else {
                        String depName = "No Department";
                        return new Tuple2<>(arr[0], depName);
                    }
                }
        );
        studentsPair.collect().forEach(x -> System.out.println(x));

        // In Condition
        JavaPairRDD<String, String> studentsPair2 = studentsRdd.mapToPair(x -> {
            String[] arr = x.split(",");
            return new Tuple2<String, String>(arr[0], arr[1]);
        }).filter(x -> {
            Map<String, String> bcVarValue = bcVar.value();
            Set<String> keys = bcVarValue.keySet();
            return keys.contains(x._2());
        });
        studentsPair2.collect().forEach(System.out::println);

    }
}
