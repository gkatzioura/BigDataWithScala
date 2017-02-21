name := "ScalaStorm"

version := "1.0"

scalaVersion := "2.12.1"

scalacOptions += "-Yresolve-term-conflict:package"

libraryDependencies += "org.apache.storm" % "storm-core" % "1.0.2"
