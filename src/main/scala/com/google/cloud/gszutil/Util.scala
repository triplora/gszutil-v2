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

import java.security.Security

import scala.util.{Failure, Try}

object Util {
  def printDebugInformation(): Unit = {
    import scala.collection.JavaConverters._
    System.out.println("\n\nSystem Properties:")
    System.getProperties.list(System.out)
    System.out.println("\n\nEnvironment Variables:")
    System.getenv.asScala.toMap.foreach{x =>
      System.out.println(s"${x._1}=${x._2}")
    }
  }

  def configureBouncyCastleProvider(): Unit = {
    import scala.collection.JavaConverters._
    Security.insertProviderAt(new org.bouncycastle.jce.provider.BouncyCastleProvider(), 1)

    Security.getProviders.foreach{p =>
      System.out.println(s"\n\n${p.getName}\t${p.getInfo}")
      p.getServices.asScala.toArray
        .map{x => s"\t${x.getType}\t${x.getAlgorithm}\t${x.getClassName}"}
        .sorted
        .mkString("\n")
        .foreach(System.out.println)
    }
  }

  def printException[T](x: Try[T]): Unit = {
    x match {
      case Failure(exception) =>
        System.err.println(exception.getMessage)
      case _ =>
    }
  }
}
