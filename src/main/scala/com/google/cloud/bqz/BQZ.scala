/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.bqz

import com.google.cloud.bqz.cmd.{Load, Mk, Query, Rm}
import com.ibm.jzos.CrossPlatform

object BQZ {
  def main(args: Array[String]): Unit = {
    val creds = CrossPlatform
      .getCredentialProvider(CrossPlatform.Keyfile)
      .getCredentials

    def fail() =
      throw new IllegalArgumentException(s"invalid args '${args.mkString(" ")}'")

    BQParser.parse(args) match {
      case Some(BQOptions(cmd, a)) =>
        cmd match {
          case "mk" =>
            MkOptionParser.parse(a) match {
              case Some(c) =>
                Mk.run(c, creds)
              case _ =>
                fail()
            }
          case "query" =>
            QueryOptionParser.parse(a) match {
              case Some(c) =>
                Query.run(c, creds)
              case _ =>
                fail()
            }
          case "load" =>
            LoadOptionParser.parse(a) match {
              case Some(c) =>
                Load.run(c, creds)
              case _ =>
                fail()
            }
          case "rm" =>
            RmOptionParser.parse(a) match {
              case Some(c) =>
                Rm.run(c, creds)
              case _ =>
                fail()
            }
          case _ =>
            fail()
        }
      case _ =>
        fail()
    }
  }
}
