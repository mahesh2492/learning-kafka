name := "learning-kafka"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.2.0",
  "com.typesafe" % "config" % "1.2.1"
)