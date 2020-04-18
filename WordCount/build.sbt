name := "WordCount"

version := "0.1"

scalaVersion := "2.12.11"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3" % "provided"
)