name := "hadoop-mapreduce-tutorial"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.1" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test

libraryDependencies += "org.mockito" % "mockito-core" % "2.8.47" % Test
