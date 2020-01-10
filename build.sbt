import sbt.Keys.{libraryDependencies, version}

lazy val root = (project in file(".")).
  settings(
    name := "GDELT-Explore",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "fr.telecom",

    publishMavenStyle := true
  )

val SparkVersion = "2.4.0"

val SparkCompatibleVersion = "2.3"

val HadoopVersion = "2.7.2"

val dependencyScope = "compile"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "com.amazonaws" % "aws-java-sdk-bom" % "1.11.703",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.703"
)