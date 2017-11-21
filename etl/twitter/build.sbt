name := "twitter"
version := "0.1"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}