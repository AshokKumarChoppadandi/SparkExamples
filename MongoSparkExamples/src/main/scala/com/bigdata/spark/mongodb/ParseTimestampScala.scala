package com.bigdata.spark.mongodb

import java.text.SimpleDateFormat
import java.util.Date

object ParseTimestampScala {
  def main(args: Array[String]): Unit = {
    val dateString = "Thu May 21 2020 11:46:28 GMT-0400 (EDT)"

    val format = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)"
    val dateFormat = new SimpleDateFormat(format)

    val date = new Date()
    val result = dateFormat.format(date)
    println("Result :: " + result)

    val date2 = dateFormat.parse(dateString)
    println("Date 2 :: " + dateFormat.format(date2))
  }
}
