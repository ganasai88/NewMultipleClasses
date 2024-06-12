ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12" // Ensure compatibility with Spark 3.5.0

lazy val root = (project in file("."))
  .settings(
    name := "pipeline",
    idePackagePrefix := Some("org.pipe.pipeline")
  )

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.6.12",
  "org.mockito" %% "mockito-scala" % "1.16.42",
  "org.scalatest" %% "scalatest-flatspec" % "3.2.18",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.3.5",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.mockito" % "mockito-core" % "2.22.0" % "test",
  "org.mockito" % "mockito-inline" % "4.5.1" % "test",
)
