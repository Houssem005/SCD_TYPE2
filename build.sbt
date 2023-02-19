ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SCD_TYPE2"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "commons-io" % "commons-io" % "2.11.0",
  "org.apache.spark" %% "spark-sql" % "3.3.1" % "compile",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test
)