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

object ScpOptionParser extends OptionParser[ScpConfig]("scp") with ArgParser[ScpConfig]{
  override def parse(args: Seq[String]): Option[ScpConfig] = parse(args, ScpConfig())

  head("scp", Bqsh.UserAgent)

  help("help").text("prints this usage text")

  opt[Long]("count")
    .optional
    .text("(optional) number of records to copy (default: unlimited)")
    .action((x,c) => c.copy(limit = x))

  opt[Unit]("noCompress")
    .optional
    .text("(optional) compress output with gzip (default: true)")
    .action((_,c) => c.copy(compress = false))

  arg[String]("inDsn")
    .required
    .text("DSN to read")
    .action((x,c) => c.copy(inDsn = x))

  arg[String]("outUri")
    .required
    .text("GCS URI to write data to")
    .action((x,c) => c.copy(outUri = x))
}
