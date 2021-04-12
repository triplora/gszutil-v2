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
version := "5.5.8"

scalaVersion := "2.13.1"

val exGuava = ExclusionRule(organization = "com.google.guava")
val exJetty = ExclusionRule(organization = "org.mortbay.jetty")
val exZk = ExclusionRule(organization = "org.apache.zookeeper")
val exNs = ExclusionRule(organization = "io.grpc", name = "grpc-netty-shaded")
val exGrpc = ExclusionRule(organization = "io.grpc")
val exAvro = ExclusionRule(organization = "org.apache.avro")

libraryDependencies ++= Seq(
  "com.google.cloud.imf" %% "mainframe-util" % "2.1.5",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)

// orc and related dependencies
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.9.2",
  "org.apache.hadoop" % "hadoop-hdfs-client" % "2.9.2",
  "org.apache.hive" % "hive-storage-api" % "2.7.1",
  "org.apache.orc" % "orc-core" % "1.6.2"
).map(_ excludeAll(exGuava,exJetty,exZk,exNs,exGrpc,exAvro))

// Don't run tests during assembly
test in assembly := Seq()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Exclude IBM jars from assembly jar since they will be provided
assemblyExcludedJars in assembly := {
  val IBMJars = Set("ibmjzos.jar", "ibmjcecca.jar", "dataaccess.jar", "isfjcall.jar")
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
  "-opt-warnings",
  "-feature"
)
