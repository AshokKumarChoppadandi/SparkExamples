name := "MongoSparkExamples"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1",
  "com.typesafe" % "config" % "1.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
