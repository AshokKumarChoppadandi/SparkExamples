package com.bigdata.spark.sharedvariables

/**
 * Created by Ashok Kumar Choppadandi
 * This is example to demonstrate the normal Join in Spark using RDDs
 * For this example Cars.csv file used and this is available at the below link:
 * https://github.com/AshokKumarChoppadandi/SparkExamples/tree/master/Data
 */

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object RegularJoin {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Start Date :: {}", new Date())
    val conf = new SparkConf().setAppName("Regular Join Example")//.setMaster("local")
    val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")

    val header = "maker,model,mileage,manufacture_year,engine_displacement,engine_power,body_type,color_slug,stk_year,transmission,door_count,seat_count,fuel_type,date_created,date_last_seen,price_eur"
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.filter(x => !x.equalsIgnoreCase(header)).map(x => x.split("\\W+"))

    val rdd3 = rdd2.map(x => (x(0), Try(x(3).toInt)))
    val makersRDD = sc.textFile(args(1))
    val makersRDDKeyValue = makersRDD.map(x => x.split("\\W+")).map(x => (x(0), x(1)))

    val joinedRDD = rdd3.join(makersRDDKeyValue)
    val finalResult = joinedRDD.map(x => x._2._2 + " " + x._2._1.getOrElse(0))
    finalResult.take(10).foreach(println)
    logger.info("End Date :: {}", new Date())
  }
}
