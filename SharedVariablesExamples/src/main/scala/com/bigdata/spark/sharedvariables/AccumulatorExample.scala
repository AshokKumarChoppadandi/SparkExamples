package com.bigdata.spark.sharedvariables

/**
 * Created by Ashok Kumar Choppadandi
 * This is example to demonstrate the Accumulators in Spark using RDDs
 * For this example Cars.csv file used and this is available at the below link:
 * https://github.com/AshokKumarChoppadandi/SparkExamples/tree/master/Data
 */

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorExample {
  def main(args: Array[String]): Unit = {
    println("Start Time :: " + new Date())
    val conf = new SparkConf().setAppName("Accumulator Example").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val longAccumulator1 = sc.longAccumulator("NullCounter")
    val longAccumulator2 = sc.longAccumulator("ValueCounter")

    val header = "maker,model,mileage,manufacture_year,engine_displacement,engine_power,body_type,color_slug,stk_year,transmission,door_count,seat_count,fuel_type,date_created,date_last_seen,price_eur"
    val rdd1 = sc.textFile("C:\\Users\\PC\\IdeaProjects\\SparkExamples\\Data\\Cars\\Cars.csv")
    //val totalRecords = rdd1.count()
    //println("Total Records :: " + totalRecords)

    val rdd2 = rdd1.filter(x => !x.equalsIgnoreCase(header)).map(x => x.split(","))
    val rdd3 = rdd2.map(x => {
      if (x(0).length == 0) {
        longAccumulator1.add(1)
      } else {
        longAccumulator2.add(1)
      }

      x
    })

    val rdd4 = rdd3.count()
    println("Total Records :: " + rdd4)
    println("Null Counter :: " + longAccumulator1.value)
    println("Value Counter :: " + longAccumulator2.value)
    println("End Time :: " + new Date())
  }
}
