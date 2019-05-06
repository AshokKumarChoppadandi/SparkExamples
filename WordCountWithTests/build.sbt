name := "WordCountWithTests"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Cloudera" at "https://repository.cloudera.com/content/repositories/releases/"

resolvers += "Spring Plugins" at "http://repo.spring.io/plugins-release/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.0-cdh5.5.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.0-cdh5.5.2"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.0-cdh5.5.2"

//libraryDependencies += "org.mortbay.jetty" % "jetty-util" % "6.1.26.cloudera.4"

//libraryDependencies += "org.cloudera.logredactor" % "logredactor" % "1.0.3"

// https://mvnrepository.com/artifact/org.scalatest/scalatest_2.10
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.1"

// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base_2.10
//libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.2_0.6.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.5.2"

// https://mvnrepository.com/artifact/org.apache.hive/hive-metastore
libraryDependencies += "org.apache.hive" % "hive-metastore" % "1.1.0-cdh5.5.2"

// https://mvnrepository.com/artifact/org.apache.hive/hive-common
libraryDependencies += "org.apache.hive" % "hive-common" % "1.1.0-cdh5.5.2"