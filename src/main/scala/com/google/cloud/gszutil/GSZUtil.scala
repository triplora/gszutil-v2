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

import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.KeyFileCredentialProvider

import scala.util.{Failure, Success, Try}

object GSZUtil {

  def main(args: Array[String]): Unit = {
    Config.parse(args) match {
      case Some(config) =>
        init(config)
        run(config) match {
          case Success(_) =>
            System.out.println("Finished")
          case Failure(exception) =>
            throw new RuntimeException(s"Failed to run with config $config", exception)
        }
      case _ =>
        System.err.println(s"Invalid args: ${args.mkString(" ")}")
        System.exit(1)
    }
  }

  def init(config: Config): Unit = {
    System.setProperty("java.net.preferIPv4Stack" , "true")

    Util.configureLogging()
    if (config.debug)
      Util.printDebugInformation()

    if (config.useBCProv)
      Util.configureBouncyCastleProvider()

    if (config.useCCA && System.getProperty("java.vm.vendor").contains("IBM"))
      Util.insertIBMJCECCAProvider()
  }

  def run(config: Config): Try[Unit] = Try{
    val cp = KeyFileCredentialProvider(KeyFile.parseFrom(Util.readB("keyfile.pb")))

    if (config.mode == "cp")
      GCSPut.run(config, cp)
    else if (config.mode == "get")
      GCSGet.run(config, cp)
    else if (config.mode == "load")
      com.google.cloud.pso.BQLoad.run(config, cp)
  }
}
