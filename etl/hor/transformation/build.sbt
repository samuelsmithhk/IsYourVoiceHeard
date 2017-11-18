name := "transformation"
version := "0.1"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}