/*
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by Ashok Kumar on 04-05-2017.
  */
class WordCountTest extends FunSuite with SharedSparkContext{

  test("testWordCountWithTests") {
    val input = sc.parallelize(Array("the cat and the bat", "the cat is blue", "the cat is in the car"))

    val wordCount: RDD[(String, Int)] = WordCountWithTests.wordCounts(input)

    assert(wordCount.count() == 8)

    val mappedValue = wordCount.collect().toMap
    assert(mappedValue.get("cat").getOrElse(0) == 3)
  }

}
*/