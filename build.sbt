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
organization := "com.google.cloud.imf"
name := "mainframe-connector"
version := "4.3.0"

scalaVersion := "2.13.1"

val exGuava = ExclusionRule(organization = "com.google.guava")
val exJetty = ExclusionRule(organization = "org.mortbay.jetty")
val exZk = ExclusionRule(organization = "org.apache.zookeeper")
val exNs = ExclusionRule(organization = "io.grpc", name = "grpc-netty-shaded")

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.1",
  "com.typesafe" %% "ssl-config-core" % "0.4.2",
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)

libraryDependencies ++= Seq("com.google.guava" % "guava" % "28.2-jre")

libraryDependencies ++= Seq(
  "com.google.api-client" % "google-api-client" % "1.30.9", // provided for google-cloud-bigquery
  "com.google.apis" % "google-api-services-logging" % "v2-rev656-1.25.0",
  "com.google.auto.value" % "auto-value-annotations" % "1.7", // provided for google-cloud-bigquery
  "com.google.http-client" % "google-http-client-apache-v2" % "1.34.2",
  "com.google.cloud" % "google-cloud-bigquery" % "1.110.0",
  "com.google.cloud" % "google-cloud-compute" % "0.117.0-alpha",
  "com.google.cloud" % "google-cloud-storage" % "1.103.1",
  "com.google.protobuf" % "protobuf-java" % "3.11.4",
  "com.google.protobuf" % "protobuf-java-util" % "3.11.4",
  "io.grpc" % "grpc-netty" % "1.28.1",
  "io.grpc" % "grpc-protobuf" % "1.28.1",
  "io.grpc" % "grpc-stub" % "1.28.1",
  "io.netty" % "netty-codec-http2" % "4.1.48.Final",
  "org.apache.hadoop" % "hadoop-common" % "2.9.2", // provided for orc-core
  "org.apache.hadoop" % "hadoop-hdfs-client" % "2.9.2", // provided for orc-core
  "org.apache.hive" % "hive-storage-api" % "2.7.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.12",
  "org.apache.orc" % "orc-core" % "1.6.2",
  "org.conscrypt" % "conscrypt-openjdk-uber" % "2.4.0"
).map(_ excludeAll(exGuava,exJetty,exZk,exNs))

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
  "-opt:l:inline",
  "-opt-inline-from:**",
  "-deprecation",
  "-opt-warnings"
)
