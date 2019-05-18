package com.bigdata.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by Ashok Kumar on 06-05-2017.
  */
trait SharedSparkContext extends BeforeAndAfterAll{self: Suite =>

  @transient private var _sc: SparkContext = _
  @transient private var _sQLContext: SQLContext = _
  @transient private var _hiveContext: HiveContext = _

  def sc: SparkContext = _sc
  def sQlContext: SQLContext = _sQLContext
  def hiveContext: HiveContext = _hiveContext

  System.setProperty("java.io.tmpdir", "C:\\tmp")
  var conf = new SparkConf(false).set("spark.local.dir", "C:\\tmp")
  //conf.set("hive.metastore.warehouse.dir", "C:\\Users\\ashok\\temp\\hive\\warehouse")
  conf.set("hive.exec.scratchdir", "C:\\tmp\\hive")
  conf.set("hive.scratch.dir.permission", "777")

  override def beforeAll(): Unit = {
    _sc = new SparkContext("local[4]", "test", conf)
    _sQLContext = new SQLContext(_sc)
    _hiveContext = new HiveContext(_sc)
    //_hiveContext.setConf("hive.metastore.warehouse.dir", "C:\\Users\\ashok\\temp\\hive\\warehouse")
    //_hiveContext.setConf("hive.exec.scratchdir", "C:\\tmp\\hive")
    //_hiveContext.setConf("hive.scratch.dir.permission", "777")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    LocalSparkContext.stop(_sc)
    _sc = null
    _sQLContext = null
    _hiveContext = null
    super.afterAll()
  }
}
