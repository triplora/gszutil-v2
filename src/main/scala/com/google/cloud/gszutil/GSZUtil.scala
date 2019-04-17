/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.gszutil

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.logging.{Level, Logger}

import com.google.cloud.gszutil.GSXML.{CredentialProvider, XMLStorage}
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.{AccessTokenCredentialProvider, KeyFileCredentialProvider, RandomInputStream}
import com.google.common.io.Resources

import scala.util.{Success, Try}

object GSZUtil {
  case class Config(dsn: String = "",
                    dest: String = "",
                    keyfile: String = "",
                    destBucket: String = "",
                    destPath: String = "",
                    mode: String = "",
                    useBCProv: Boolean = true,
                    debug: Boolean = true)

  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("GSZUtil") {
      head("GSZUtil", "0.1")

      cmd("cp")
        .action{(_, c) => c.copy(mode = "cp")}
        .text("GSZUtil cp copies a zOS dataset to GCS")
        .children(
          arg[String]("dsn")
            .required()
            .action{(x, c) => c.copy(dsn = x)}
            .text("record to be copied"),
          arg[String]("dest")
            .required()
            .action{(x, c) =>
              Try(Util.parseUri(x)) match {
                case Success((bucket, path)) =>
                  c.copy(dest = x, destBucket = bucket, destPath = path)
                case _ =>
                  c.copy(dest = x)
              }
            }
            .text("destination path (gs://bucket/path)"),
          arg[String]("keyfile")
            .action{(x, c) => c.copy(keyfile = x)}
            .text("path to keyfile.pb")
        )
      checkConfig(c =>
        if (c.destBucket.isEmpty || c.destPath.isEmpty)
          failure(s"invalid destination '${c.dest}'")
        else if (c.keyfile.nonEmpty && !Paths.get(c.keyfile).toFile.exists())
          failure(s"keyfile '${c.keyfile}' doesn't exist")
        else success
      )
    }

  private val logger: Logger = Logger.getLogger(this.getClass.getSimpleName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {
    System.out.println(s"Running with args: ${args.mkString(" ")}")
    Parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
        System.out.println(s"Invalid args: ${args.mkString(" ")}")
    }
  }

  def run(config: Config): Unit = {
    if (config.debug) {
      Util.printDebugInformation()
      Util.configureLogging()
    } else Util.configureLogging(Level.INFO)
    if (config.useBCProv) Util.configureBouncyCastleProvider()

    val keyFile: KeyFile = Try{
      KeyFile.parseFrom(Resources.toByteArray(Resources.getResource("keyfile.pb")))
    }.getOrElse(KeyFile.parseFrom(Util.readNio(config.keyfile)))

    val cp: CredentialProvider = if (keyFile.getAccessToken.nonEmpty) {
      AccessTokenCredentialProvider(keyFile.getAccessToken)
    } else {
      KeyFileCredentialProvider(keyFile)
    }
    val gcs = XMLStorage(cp)

    System.out.println(s"Uploading 100MB of random data for speed test")
    put(gcs, new RandomInputStream(100000000), config.destBucket, "test_data_100MB")

    System.out.println(s"Uploading ${config.dsn} to ${config.dest}")
    put(gcs, ZReader.read(config.dsn), config.destBucket, config.destPath)
  }

  def put(gcs: XMLStorage, in: InputStream, bucket: String, path: String): Unit = {
    val request = gcs.putObject(
      bucket = bucket,
      key = path,
      inputStream = in)

    val startTime = System.currentTimeMillis()
    val response = request.execute()
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000L

    if (response.isSuccessStatusCode){
      logger.info(s"Success ($duration seconds)")
    } else {
      logger.warning(s"Error: Status code ${response.getStatusCode}\n${response.parseAsString}")
    }
  }
}
