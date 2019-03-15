name := "gszutil"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "27.0.1-jre",
  "com.google.cloud" % "google-cloud-storage" % "1.65.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.9.8",
  "com.ibm.jzos" % "jzos" % "1.0" % Provided from "file:///opt/zjdk/lib/ext/ibmjzos.jar",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
) 

mainClass in assembly := Some("com.google.cloud.gszutil.GSZUtil")
