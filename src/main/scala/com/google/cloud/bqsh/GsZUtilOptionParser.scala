/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.bqsh

import scopt.OptionParser

object GsZUtilOptionParser extends OptionParser[GsZUtilConfig]("gszutil")
with ArgParser[GsZUtilConfig]{
  override def parse(args: Seq[String]): Option[GsZUtilConfig] = {
    val envCfg = GsZUtilConfig(
      gcsOutUri = sys.env.getOrElse("GCSOUTURI", ""),
      remoteHost = sys.env.getOrElse("SRVHOST",""),
      remotePort = sys.env.getOrElse("SRVPORT","51770").toInt,
    )
    parse(args, envCfg)
  }

  head("gszutil", Bqsh.UserAgent)

  help("help").text("prints this usage text")

  opt[String]("inDsn")
    .optional
    .text("DSN to read")
    .action((x,c) => c.copy(inDsn = x))

  opt[String]("gcsOutUri")
    .optional
    .text("Cloud Storage prefix for output ORC files (format: gs://BUCKET/PREFIX")
    .action((x,c) => c.copy(gcsOutUri = x))
}
