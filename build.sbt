ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "kudutopostgresql",
      libraryDependencies +="org.apache.spark" % "spark-core_2.12" % "3.2.1",
      libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.2.1",
      libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.2.1" % "provided",
      libraryDependencies += "org.apache.kudu" %% "kudu-spark3" % "1.16.0"
)
assemblyMergeStrategy in assembly := {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case x => MergeStrategy.first
}
