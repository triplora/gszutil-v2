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

package com.google.cloud.imf

import com.google.api.services.logging.v2.LoggingScopes
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.imf.grecv.GRecvConfigParser
import com.google.cloud.imf.grecv.server.GRecvServer
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.{CloudLogging, Logging, Services}

/** The server side of the mainframe connector
  * Receives requests to transcode to ORC
  */
object GRecv extends Logging {
  val BatchSize = 1024
  val PartitionBytes: Long = 128L * 1024 * 1024

  def main(args: Array[String]): Unit = {
    val buildInfo = "Build Info:\n" + Util.readS("build.txt")
    System.out.println(buildInfo)
    GRecvConfigParser.parse(args) match {
      case Some(cfg) =>
        val zos = Util.zProvider
        zos.init()
        val creds = GoogleCredentials.getApplicationDefault
        CloudLogging.configureLogging(debugOverride = cfg.debug, sys.env,
          errorLogs = Seq("org.apache.orc","io.grpc","io.netty","org.apache.http"),
          credentials = creds.createScoped(LoggingScopes.LOGGING_WRITE))
        logger.info(s"Starting GRecvServer\n$buildInfo")
        val gcs = Services.storage(creds.createScoped(StorageScopes.DEVSTORAGE_READ_WRITE))
        new GRecvServer(cfg, gcs).start()
      case _ =>
        System.err.println(s"Unabled to parse args '${args.mkString(" ")}'")
        System.exit(1)
    }
  }
}
