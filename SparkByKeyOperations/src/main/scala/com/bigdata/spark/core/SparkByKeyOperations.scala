package com.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkByKeyOperations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark By Key Operations").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = sc.textFile("D:\\Study_Job_Knowledge\\Data\\flight-delays\\flights.csv")

    val filteredData = data.filter(x => !x.startsWith("YEAR")).map(x => x.split(","))
    val flightWithDistance = filteredData.map(x => (x(5), x(17).toInt))

    // Top 10 flights with high Average Distance travelled

    // 1. Using Group By Key Operation
    println("Top 10 Flights with Average Distance Travelled - Using Group By Key Operation")
    val groupByFlight = flightWithDistance.groupByKey()
    val distanceAndTripsTravelledByFlight1 = groupByFlight.map(x => (x._1, (x._2.sum, x._2.size)))
    val averageDistanceTravelledByFlight1 = distanceAndTripsTravelledByFlight1.map(x => (x._1, x._2._1.toDouble / x._2._2))
    val flightsSortedByDistanceTravelled1 = averageDistanceTravelledByFlight1.sortBy(x => x._2, false)
    val top10FlightsWithAvergareDistance1 = flightsSortedByDistanceTravelled1.take(10)
    top10FlightsWithAvergareDistance1.foreach(println)

    // 2. Using Reduce By key
    println("Top 10 Flights with Average Distance Travelled - Using Reduce By Key Operation")
    val totalDistanceTravelledByEachFlight = flightWithDistance.reduceByKey((distance1, distance2) => distance1 + distance2)
    val totalTripsByEachFlight = flightWithDistance.map(x => (x._1, 1)).reduceByKey((trip1, trip2) => trip1 + trip2)
    val joinDataTotalDistanceAndTrips = totalDistanceTravelledByEachFlight.join(totalTripsByEachFlight)
    val averageDistanceTravelledByFlight2 = joinDataTotalDistanceAndTrips.map(x => (x._1, x._2._1.toDouble / x._2._2))
    val flightsSortedByDistanceTravelled2 = averageDistanceTravelledByFlight2.sortBy(x => x._2, false)
    val top10FlightsWithAvergareDistance2 = flightsSortedByDistanceTravelled2.take(10)
    top10FlightsWithAvergareDistance2.foreach(println)

    // 3. Using Aggregate By key
    println("Top 10 Flights with Average Distance Travelled - Using Aggregate By Key Operation")
    val distanceAndTripsTravelledByFlight3 = flightWithDistance.aggregateByKey(
      (0.0, 0)
    )(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )
    val averageDistanceTravelledByFlight3 = distanceAndTripsTravelledByFlight3.map(x => (x._1, x._2._1.toDouble / x._2._2))
    val flightsSortedByDistanceTravelled3 = averageDistanceTravelledByFlight3.sortBy(x => x._2, false)
    val top10FlightsWithAvergareDistance3 = flightsSortedByDistanceTravelled3.take(10)
    top10FlightsWithAvergareDistance3.foreach(println)

    // 4. Using Combine By key
    println("Top 10 Flights with Average Distance Travelled - Using Combine By Key Operation")
    val distanceAndTripsTravelledByFlight4 = flightWithDistance.combineByKey(
      (x: Int) => (0, 0),
      (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
    )
    val averageDistanceTravelledByFlight4 = distanceAndTripsTravelledByFlight4.map(x => (x._1, x._2._1.toDouble / x._2._2))
    val flightsSortedByDistanceTravelled4 = averageDistanceTravelledByFlight4.sortBy(x => x._2, false)
    val top10FlightsWithAvergareDistance4 = flightsSortedByDistanceTravelled4.take(10)
    top10FlightsWithAvergareDistance4.foreach(println)

  }
}
