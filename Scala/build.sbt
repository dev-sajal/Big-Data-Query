name := "ScalaFirst"

version := "0.1"

scalaVersion := "2.11.6"


val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
 
)