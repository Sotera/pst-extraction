name := "tika extract"
version := "1.0.1"
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.tika" % "tika-core" % "1.10"
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.10"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"

