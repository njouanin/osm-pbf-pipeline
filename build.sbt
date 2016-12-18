organization := "org.tripod"
name := "osm-pbf-pipeline"
version := "1.0"

scalaVersion := "2.12.1"
crossScalaVersions := Seq(scalaVersion.value, "2.11.8", "2.12.1")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"         % "2.4.14",
  "org.scalatest"     %% "scalatest"           % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.14" % "test",
  "com.typesafe.akka" %% "akka-http"           % "10.0.0"
)

PB.targets in Compile := Seq(
  scalapb.gen(grpc = false) -> (sourceManaged in Compile).value
)
