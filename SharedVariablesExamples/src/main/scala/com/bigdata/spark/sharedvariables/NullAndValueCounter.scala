package com.bigdata.spark.sharedvariables

/**
 * Created by Ashok Kumar Choppadandi
 * This is example to demonstrate the Counting the records without Accumulators in Spark using RDDs
 * For this example Cars.csv file used and this is available at the below link:
 * https://github.com/AshokKumarChoppadandi/SparkExamples/tree/master/Data
 */

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object NullAndValueCounter {
  def main(args: Array[String]): Unit = {
    println("Start Time :: " + new Date())
    val conf = new SparkConf().setAppName("Accumulator Example").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val header = "maker,model,mileage,manufacture_year,engine_displacement,engine_power,body_type,color_slug,stk_year,transmission,door_count,seat_count,fuel_type,date_created,date_last_seen,price_eur"
    val rdd1 = sc.textFile("C:\\Users\\PC\\IdeaProjects\\SparkExamples\\Data\\Cars\\Cars.csv")
    val rdd2 = rdd1.filter(x => !x.equalsIgnoreCase(header)).map(x => x.split(","))

    val totalCount = rdd2.count()
    val nullCounter = rdd2.filter(x => x(0).length == 0).count()
    val valueCounter = rdd2.filter(x => x(0).length != 0).count()

    println("Total Records :: " + totalCount)
    println("Null Counter :: " + nullCounter)
    println("Value Counter :: " + valueCounter)
    println("End Time :: " + new Date())
  }
}
