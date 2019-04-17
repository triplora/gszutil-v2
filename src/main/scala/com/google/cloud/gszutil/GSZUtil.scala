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

import java.nio.file.Paths
import java.util.logging.{Level, Logger}

import com.google.cloud.gszutil.GSXML.XMLStorage
import com.google.cloud.gszutil.Util.JSONCredentialProvider

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
            .required()
            .action{(x, c) => c.copy(keyfile = x)}
            .text("path to json credentials keyfile")
        )
      checkConfig(c =>
        if (c.destBucket.isEmpty || c.destPath.isEmpty)
          failure(s"invalid destination '${c.dest}'")
        else if (!Paths.get(c.keyfile).toFile.exists())
          failure(s"keyfile '${c.keyfile}' doesn't exist")
        else success
      )
    }

  private val logger: Logger = Logger.getLogger(this.getClass.getSimpleName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {
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

    val gcs = XMLStorage(JSONCredentialProvider(Util.readNio(config.keyfile)))

    logger.info(s"Uploading ${config.dsn} to ${config.dest}")
    val request = gcs.putObject(
      bucket = config.destBucket,
      key = config.destPath,
      inputStream = ZReader.read(config.dsn))

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
