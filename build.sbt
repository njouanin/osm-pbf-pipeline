organization := "org.tripod"
name := "osm-pbf-pipeline"
licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
startYear := Some(2016)
resolvers += Resolver.jcenterRepo

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

scmInfo :=
  Some(
    ScmInfo(
      url("https://github.com/tripod-oss/osm-pbf-pipeline"),
      "scm:git:git@github.com:tripod-oss/osm-pbf-pipeline.git"
    ))

// Publish
bintrayOrganization := Some("tripod")
publishMavenStyle := true
publishArtifact in Test := false
pomExtra in Global := {
  <developers>
      <developer>
        <name>Nicolas Jouanin</name>
        <email>nico@tripod.traval</email>
        <organization>Tripod</organization>
        <organizationUrl></organizationUrl>
      </developer>
    </developers>
}
