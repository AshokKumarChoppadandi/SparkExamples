name := "MongoSparkExamples"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1",
  "com.typesafe" % "config" % "1.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("spark-core") ||
      f.data.getName == "spark-core_2.11-2.4.4.jar"
  }

  cp filter { f =>
    f.data.getName.contains("spark-sql") ||
    f.data.getName == "spark-sql_2.11-2.4.4.jar"
  }
}