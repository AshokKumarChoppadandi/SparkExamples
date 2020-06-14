name := "WordCount"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  //"org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.0" % "provided",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.11.0" % "provided",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0" % "provided"
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