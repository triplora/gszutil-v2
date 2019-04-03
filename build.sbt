name := "gszutil"

scalaVersion := "2.11.8"

organization := "com.google.cloud"

version := "0.1.0-SNAPSHOT"

val jzosPath = "/opt/J8.0_64/lib/ext/ibmjzos.jar"

val exGuava = ExclusionRule(organization = "com.google.guava")

libraryDependencies ++= Seq(
  "com.google.api-client" % "google-api-client" % "1.27.0" excludeAll exGuava,
  "com.google.guava" % "guava" % "27.0.1-jre",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.9.8",
  "com.ibm.jzos" % "jzos" % "1.0" % Provided from s"file://$jzosPath",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
) 

mainClass in assembly := Some("com.google.cloud.gszutil.GSZUtil")

assemblyJarName in assembly := "gszutil.jar"

// Don't run tests during assembly
test in assembly := Seq()
