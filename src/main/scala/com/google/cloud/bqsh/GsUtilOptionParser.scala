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

package com.google.cloud.bqsh

import java.net.URI

import scopt.OptionParser


object GsUtilOptionParser extends OptionParser[GsUtilConfig]("gsutil") {
  private val DefaultConfig = GsUtilConfig()
  def parse(args: Seq[String]): Option[GsUtilConfig] = parse(args, DefaultConfig)

  head("gsutil", "0.2.1")

  help("help").text("prints this usage text")

  cmd("cp")
    .text("Upload Binary MVS Dataset to GCS")

    .children(
      arg[String]("source")
        .required()
        .text("Source DDNAME")
        .action((x,c) => c.copy(source = x)),

      arg[String]("destinationUri")
        .required()
        .text("Destination URI (gs://bucket/path)")
        .validate{x =>
          val uri = new URI(x)
          if (uri.getScheme != "gs" || uri.getAuthority.isEmpty)
            failure("invalid GCS URI")
          else
            success
        }
        .action((x, c) => c.copy(destinationUri = x))
    )

  opt[Int]("partSizeMB")
    .action{(x,c) => c.copy(partSizeMB = x)}
    .text("target part size in megabytes (default: 256)")

  opt[Int]("batchSize")
    .action{(x,c) => c.copy(blocksPerBatch = x)}
    .text("blocks per batch (default: 1000)")

  opt[Int]('p', "parallelism")
    .action{(x,c) => c.copy(parallelism = x)}
    .text("number of concurrent writers (default: 5)")

  opt[Int]("timeOutMinutes")
    .action{(x,c) => c.copy(timeOutMinutes = x)}
    .text("timeout in minutes (default: 180)")

  opt[String]("keyfile")
    .action{(x,c) => c.copy(copyBook = x)}
    .text("Keyfile DDNAME (default: KEYFILE)")

  opt[String]("copybook")
    .action{(x,c) => c.copy(copyBook = x)}
    .text("Copybook DDNAME (default: COPYBOOK)")
}
