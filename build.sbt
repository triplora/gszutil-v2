name := "gszutil"

scalaVersion := "2.11.8"

organization := "com.google.cloud"

version := "0.1.0-SNAPSHOT"

val jzosPath = "/opt/J8.0_64/lib/ext/ibmjzos.jar"

val exGuava = ExclusionRule(organization = "com.google.guava")

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "27.0.1-jre"
)

libraryDependencies ++= Seq(
  "com.google.api-client" % "google-api-client" % "1.28.0",
  "com.google.cloud" % "google-cloud-bigquery" % "1.71.0",
  "com.google.cloud" % "google-cloud-storage" % "1.71.0",
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.16",
  "com.google.protobuf" % "protobuf-java" % "3.7.1",
  "com.google.protobuf" % "protobuf-java-util" % "3.7.1",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.9.8",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.61",
  "com.ibm.jzos" % "jzos" % "1.0" % Provided from s"file://$jzosPath",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
).map(_ excludeAll exGuava)

mainClass in assembly := Some("com.google.cloud.gszutil.GSZUtil")

assemblyJarName in assembly := "gszutil.jar"
assemblyJarName in assemblyPackageDependency := "gszutil.dep.jar"

// Don't run tests during assembly
test in assembly := Seq()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}