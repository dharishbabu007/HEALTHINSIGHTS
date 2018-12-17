
name := "NcqaHedisGICTempLoad"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" exclude("commons-codec" , "commons-codec"),
  "org.apache.spark" %% "spark-sql" % "2.3.0" exclude("commons-codec" , "commons-codec")

)