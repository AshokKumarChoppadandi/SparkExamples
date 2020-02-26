package com.bigdata.spark.sharedvariables

/**
 * Created by Ashok Kumar Choppadandi
 * This is example to demonstrate the Broadcast join in Spark using RDDs
 * For this example Cars.csv file used and this is available at the below link:
 * https://github.com/AshokKumarChoppadandi/SparkExamples/tree/master/Data
 */

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object BroadcastJoin {
  def main(args: Array[String]): Unit = {
    println("Start Date :: " + new Date())
    val conf = new SparkConf().setAppName("Accumulator Example").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val header = "maker,model,mileage,manufacture_year,engine_displacement,engine_power,body_type,color_slug,stk_year,transmission,door_count,seat_count,fuel_type,date_created,date_last_seen,price_eur"
    val rdd1 = sc.textFile("C:\\Users\\PC\\IdeaProjects\\SparkExamples\\Data\\Cars\\Cars.csv")
    val rdd2 = rdd1.filter(x => !x.equalsIgnoreCase(header)).map(x => x.split(","))

    val rdd3 = rdd2.map(x => (x(0), Try(x(3).toInt)))
    val makersRDD = sc.textFile("C:\\Users\\PC\\IdeaProjects\\SparkExamples\\SharedVariablesExamples\\src\\main\\resources\\makers.csv")
    val makersRDDKeyValue = makersRDD.map(x => x.split(",")).map(x => (x(0), x(1)))
    val bcVar = makersRDDKeyValue.collectAsMap()

    sc.broadcast(bcVar)

    val rdd4 = rdd3.map(x => {
      if(bcVar.contains(x._1)) {
        val result = bcVar.getOrElse(x._1, "OTHERS")
        (result + " " + x._2.getOrElse(0), true)
      } else {
        (x._1, false)
      }
    })

    val rdd5 = rdd4.filter(x => x._2)
    rdd5.map(x => x._1).take(10).foreach(println)
    println("End Date :: " + new Date())

  }
}
