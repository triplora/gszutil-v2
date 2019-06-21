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

import scopt.OptionParser


object BQParser extends OptionParser[BQOptions]("bq") {
  def parse(args: Seq[String]): Option[BQOptions] =
    parse(args, BQOptions())

  override def parse(args: Seq[String], init: BQOptions): Option[BQOptions] = {
    if (args.length > 2 && args.head == "bq"){
      val globalArgs = args.takeWhile{s => s == "bq" || s.startsWith("--")}
      val cmdArgs = args.dropWhile(s => s == "bq" || s.startsWith("--"))
      if (cmdArgs.length > 1)
        Option(BQOptions(cmdArgs.head, globalArgs.drop(1) ++ cmdArgs.drop(1)))
      else None
    } else None
  }

  head("bq")

  help("help")
    .text("prints this usage text")

  arg[String]("bq")
    .required()

  arg[String]("cmd")
    .required()
    .action((x,c) => c.copy(cmd = x))

  arg[Seq[String]]("args")
    .required()
    .action((x,c) => c.copy(args = x))
}
