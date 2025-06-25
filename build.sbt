name := "MalaysiaGTFSHistoryAnalysis"

version := "1.0"

scalaVersion := "2.13.16" // Match your Spark version

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0"

Compile / unmanagedSourceDirectories := (Compile / unmanagedSourceDirectories).value.filterNot(_.getPath.endsWith("/java"))
