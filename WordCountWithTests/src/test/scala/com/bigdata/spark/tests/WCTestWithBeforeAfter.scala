/*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Ashok Kumar on 04-05-2017.
  */

class WCTestWithBeforeAfter extends FunSuite with BeforeAndAfterAll{
  private var conf: SparkConf = _
  private var sc: SparkContext = _
  private var sQLContext: SQLContext = _

  override def beforeAll(): Unit = {
    conf = new SparkConf().setAppName("Word Count Unit Testing").setMaster("local")
    sc = new SparkContext(conf)
    sQLContext = new SQLContext(sc)
  }

  private val wc = new WordCount

  //val rdd: RDD[String] = sc.parallelize(List("ashok kumar", "kumar choppadandi", "ashok kumar choppadandi"))
  //sc.broadcast(rdd)
  test("Test RDD Word Count") {
    val res: RDD[(String, Int)] = wc.wordCounts(sc, "C:\\Users\\ashok\\Desktop\\Test.txt")
    assert(res.count() == 3 )

    val res1 = res.collect().toMap
    assert(res1.get("ashok").getOrElse(0) == 2)
  }

  test("TEST Data Frame Count") {
    val resDf: DataFrame = wc.getDf(sQLContext, "C:\\Users\\ashok\\Desktop\\Test.txt")
    val resCount = resDf.count()

    assert(resCount == 3)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }
}
*/