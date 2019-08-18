name := "gszutil"

scalaVersion := "2.11.8"

organization := "com.google.cloud"

version := "1.0.0"

val exGuava = ExclusionRule(organization = "com.google.guava")

libraryDependencies ++= Seq("com.google.guava" % "guava" % "28.0-jre")

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % "4.5.9",
  "com.google.http-client" % "google-http-client-apache-v2" % "1.30.2",
  "com.google.api-client" % "google-api-client" % "1.30.2",
  "com.google.cloud" % "google-cloud-bigquery" % "1.82.0",
  "com.google.cloud" % "google-cloud-storage" % "1.82.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.22",
  "org.apache.orc" % "orc-core" % "1.5.5",
  "org.apache.hive" % "hive-storage-api" % "2.6.0",
  "com.google.protobuf" % "protobuf-java" % "3.7.1",
  "com.google.protobuf" % "protobuf-java-util" % "3.7.1",
  "com.github.scopt" %% "scopt" % "3.7.1",
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

// Exclude IBM jars from assembly jar since they will be provided
assemblyExcludedJars in assembly := {
  val IBMJars = Set("ibmjzos.jar", "ibmjcecca.jar", "dataaccess.jar")
  (fullClasspath in assembly).value
    .filter(file => IBMJars.contains(file.data.getName))
}

publishMavenStyle := false