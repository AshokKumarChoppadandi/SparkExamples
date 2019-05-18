package com.bigdata.spark.testing

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
  * Created by Ashok Kumar on 06-05-2017.
  */
trait LocalSparkContext extends BeforeAndAfterEach
  with BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

  override def afterEach(): Unit = {
    resetSparkContext()
    super.afterEach()
  }
}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    Option(sc).foreach(_.stop())
    System.clearProperty("spark.driver.port")
  }

  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try{
      f(sc)
    } finally {
      stop(sc)
    }
  }

}