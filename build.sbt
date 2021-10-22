name := "FlightDataAssignment"

version := "0.1"

scalaVersion := "2.12.13"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// Scala Test dependencies - https://www.jetbrains.com/help/idea/run-debug-and-test-scala.html#test_scala_app
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test

