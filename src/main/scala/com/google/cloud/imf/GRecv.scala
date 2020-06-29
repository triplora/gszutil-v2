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
import com.google.cloud.imf.grecv.GRecvConfigParser
import com.google.cloud.imf.grecv.grpc.GrpcReceiver
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.{CloudLogging, Logging, SecurityUtils}

object GRecv extends Logging {
  val BatchSize = 1024
  val PartitionBytes: Long = 128L * 1024 * 1024

  def main(args: Array[String]): Unit = {
    val buildInfo = "Build Info:\n" + Util.readS("build.txt")
    GRecvConfigParser.parse(args) match {
      case Some(cfg) =>
        val zos = Util.zProvider
        zos.init()
        CloudLogging.configureLogging(debugOverride = false, sys.env,
          errorLogs = Seq("org.apache.orc","io.grpc","io.netty","org.apache.http"),
          credentials = zos.getCredentialProvider().getCredentials.createScoped(LoggingScopes.LOGGING_WRITE))
        logger.info(buildInfo)
        SecurityUtils.useConscrypt()
        val result = GrpcReceiver.run(cfg)
        if (result.exitCode != 0)
          System.err.println("Receiver returned non-zero exit code")
        System.exit(result.exitCode)
      case _ =>
        System.out.println(buildInfo)
        System.err.println(s"Unabled to parse args '${args.mkString(" ")}'")
        System.exit(1)
    }
  }
}
