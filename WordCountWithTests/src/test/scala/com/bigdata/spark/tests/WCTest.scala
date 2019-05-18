/*
import SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by Ashok Kumar on 07-06-2017.
  */
class WCTest  extends FunSuite with SharedSparkContext {

  test("WordCount Test") {
    val rdd: RDD[String] = sc.parallelize(Array("XYZ ABC", "ABC PQR", "ABC PQR XYZ"))
    val wordCounts: RDD[(String, Int)] = WordCountWithTests.wordCounts(rdd)
    assert(wordCounts.count() == 3)
  }

  test("getHiveTableTest") {
    val loc = "C:\\Users\\ashok\\Desktop\\Test.txt"
    val res = WordCountWithTests.getHiveTable(hiveContext, loc)

    assert(res.equalsIgnoreCase("HiveTable"))
  }
}

*/