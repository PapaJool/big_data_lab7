ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "datamart"
  )

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1"
