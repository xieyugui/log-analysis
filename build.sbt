name := "bigdata"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.0-M2",
  "io.spray" %  "spray-json_2.10" % "1.3.2"

)
    