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

import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.CrossPlatform

import scala.util.{Failure, Success, Try}

object GSZUtil extends Logging {

  def main(args: Array[String]): Unit = {
    ConfigParser.parse(args) match {
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
    CrossPlatform.init()
    System.setProperty("java.net.preferIPv4Stack" , "true")

    Util.configureLogging(config.debug)
  }

  def run(config: Config): Try[Unit] = Try{
    val cp = CrossPlatform.getCredentialProvider(config.keyFileDD)
    val creds = cp.getCredentials

    if (config.mode == "cp")
      GCSPut.run(config, creds)
    else if (config.mode == "get")
      GCSGet.run(config, creds)
    else if (config.mode == "load")
      BQLoad.run(config, creds)
    else if (config.mode == "query")
      RunQueries.run(config, creds)
  }
}
