/*
 * Copyright 2019 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
organization := "com.google.cloud"
name := "gszutil"
version := "3.1.0"

scalaVersion := "2.11.12"

val exGuava = ExclusionRule(organization = "com.google.guava")

libraryDependencies ++= Seq("com.google.guava" % "guava" % "28.2-jre")

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.1",
  "com.google.api-client" % "google-api-client" % "1.30.2", // provided for google-cloud-bigquery
  "com.google.auto.value" % "auto-value-annotations" % "1.7", // provided for google-cloud-bigquery
  "com.google.http-client" % "google-http-client-apache-v2" % "1.34.1",
  "com.google.cloud" % "google-cloud-bigquery" % "1.106.0",
  "com.google.cloud" % "google-cloud-compute" % "0.117.0-alpha",
  "com.google.cloud" % "google-cloud-storage" % "1.103.1",
  "com.google.protobuf" % "protobuf-java" % "3.11.3",
  "com.google.protobuf" % "protobuf-java-util" % "3.11.3",
  "com.typesafe.akka" %% "akka-actor" % "2.5.29",
  "org.apache.hadoop" % "hadoop-common" % "2.9.2", // provided for orc-core
  "org.apache.hadoop" % "hadoop-hdfs-client" % "2.9.2", // provided for orc-core
  "org.apache.hive" % "hive-storage-api" % "2.7.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.11",
  "org.apache.orc" % "orc-core" % "1.6.2",
  "org.zeromq" % "jeromq" % "0.5.2",
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

publishMavenStyle := true

resourceGenerators in Compile += Def.task {
  val file = (resourceDirectory in Compile).value / "build.txt"
  IO.write(file, new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date))
  Seq(file)
}.taskValue

scalacOptions ++= Seq(
  "-optimize"
)
