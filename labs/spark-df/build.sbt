assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

name := "spark-df"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1"

)
