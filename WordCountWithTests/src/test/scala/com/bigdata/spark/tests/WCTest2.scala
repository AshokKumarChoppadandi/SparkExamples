import com.bigdata.spark.testing.SharedSparkContext
import com.bigdata.spark.{WordCount, WordCountWithTests}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FunSuite

/**
  * Created by Ashok Kumar on 06-05-2017.
  */
class WCTest2 extends FunSuite with SharedSparkContext{

  test("testWordCount1") {
    val rdd: RDD[String] = sc.parallelize(Array("Ashok Kumar", "Kumar Ashok", "Ashok Kumar Ashok"))
    val wordCounts: RDD[(String, Int)] = WordCountWithTests.wordCounts(rdd)
    assert(wordCounts.count() == 3)
  }

  test("testWordCount2") {
    val wc = new WordCount
    val wordCounts = wc.wordCounts(sc, "C:\\Users\\ashok\\Desktop\\Test.txt")
    assert(wordCounts.count() == 3)
  }

  private val wc = new WordCount

  test("TEST Data Frame Count") {
    val sqlContext = sQlContext
    val resDf: DataFrame = wc.getDf(sqlContext, "C:\\Users\\ashok\\Desktop\\Test.txt")
    val resCount = resDf.count()

    assert(resCount == 3)
  }

  def getTempDf(sQLContext: SQLContext, path: String): DataFrame = {
    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    import sQLContext.implicits._
    val data = sQLContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = sQLContext.createDataFrame(rdd, schema)
    df
  }

  test("TEST Temp Table Count") {
    val sqlContext = sQlContext
    val tabName: String = wc.getTempTable(sqlContext, "C:\\Users\\ashok\\Desktop\\Test.txt")
    val query = s"SELECT * FROM $tabName"
    val df = sqlContext.sql(query).count()

    assert(df == 3)
  }

  test("Testing Data Frames") {

    val sqlContext = sQlContext
    val resDf1: DataFrame = getTempDf(sqlContext, "C:\\Users\\ashok\\Desktop\\Test.txt")
    val resDf2: DataFrame = wc.getDf(sqlContext, "C:\\Users\\ashok\\Desktop\\Test.txt")

    val resDf = resDf1.except(resDf2)
    assert(resDf.count() == 0)
  }


  def getHiveTempTable(hiveContext: HiveContext, path: String): String = {
    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )

    val data = hiveContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = hiveContext.createDataFrame(rdd, schema)

    val tempTab = "HiveTable2"
    df.registerTempTable(tempTab)

    tempTab
  }

  test("Testing Hive Tables") {
    try {
      val res1 = wc.getHiveTable(hiveContext, "C:\\Users\\ashok\\Desktop\\Test.txt")
      val res2 = getHiveTempTable(hiveContext, "C:\\Users\\ashok\\Desktop\\Test.txt")

      val query1 = s"select * from $res1"
      val query2 = s"select * from $res2"

      val df1 = hiveContext.sql(query1)
      val df2 = hiveContext.sql(query2)

      assert(df1.count() == df2.count())
    } catch {
      case ex: Exception => println("Shutting down the Spark application")
    }
  }

  def getHiveDBTempTable(hiveContext: HiveContext, path: String, dbName: String): String = {
    val schema = StructType(
      List(StructField("FirstName", StringType, true), StructField("LastName", StringType, true))
    )
    hiveContext.sql(s"create database if not exists $dbName")
    hiveContext.sql(s"use $dbName")
    val data = hiveContext.sparkContext.textFile(path)
    val rdd = data.map(x => x.split(" ")).map(x => Row(x(0), x(1)))
    val df = hiveContext.createDataFrame(rdd, schema)

    val tempTab = "HiveTable2"
    df.registerTempTable(tempTab)

    val testQuery =
      """
        | CREATE TABLE TEMP1 (fname string, lname string)
      """.stripMargin
    hiveContext.sql(testQuery)

    val insertQuery =
      """
        | INSERT INTO TABLE TEMP1
        | SELECT * FROM HiveTable2
      """.stripMargin
    hiveContext.sql(insertQuery)

    "TEMP1"
  }

  test("Testing Hive Database") {
    val res1 = wc.getHiveDBTable(hiveContext, "C:\\Users\\ashok\\Desktop\\Test.txt", "temp_db")
    val res2 = getHiveDBTempTable(hiveContext, "C:\\Users\\ashok\\Desktop\\Test.txt", "temp_db")

    val query1 = s"select * from $res1"
    val query2 = s"select * from $res2"

    val df1 = hiveContext.sql(query1)
    val df2 = hiveContext.sql(query2)

    assert(df1.count() == df2.count())
  }

  test("Testing Variable") {
    assert(WordCountWithTests.tempVar.equalsIgnoreCase("ashok"))
  }
}
