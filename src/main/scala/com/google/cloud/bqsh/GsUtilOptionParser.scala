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

package com.google.cloud.bqsh

import java.net.URI

import scopt.OptionParser


object GsUtilOptionParser extends OptionParser[GsUtilConfig]("gsutil") with ArgParser[GsUtilConfig] {
  def main(args: Array[String]): Unit =
    this.parse(("cp gs://bucket/app.jar /path/to/app.jar").split(" ").toIndexedSeq) match {
      case Some(value) =>
        System.out.println(value)
      case None =>
    }

  def parse(args: Seq[String]): Option[GsUtilConfig] =
    parse(args, GsUtilConfig())

  head("gsutil", Bqsh.UserAgent)

  help("help").text("prints this usage text")

  cmd("cp")
    .text("Upload Binary MVS Dataset to GCS")
    .action((_,c) => c.copy(mode = "cp"))
    .children(
      opt[Unit]("replace")
        .optional()
        .action{(_,c) => c.copy(replace = true, recursive = true)}
        .text("delete before uploading"),

      opt[Int]("partSizeMB")
        .optional()
        .action{(x,c) => c.copy(partSizeMB = x)}
        .text("target part size in megabytes (default: 256)"),

      opt[Int]("batchSize")
        .optional()
        .action{(x,c) => c.copy(blocksPerBatch = x)}
        .text("blocks per batch (default: 1000)"),

      opt[Unit]('m', "parallel")
        .optional()
        .action{(_,c) => c.copy(parallelism = 4)}
        .text("number of concurrent writers (default: 4)"),

      opt[Int]('p', "parallelism")
        .optional()
        .action{(x,c) => c.copy(parallelism = x)}
        .text("number of concurrent writers (default: 4)"),

      opt[Int]("timeOutMinutes")
        .optional()
        .action{(x,c) => c.copy(timeOutMinutes = x)}
        .text("(optional) Timeout in minutes. (default: 1 day)"),

      opt[Unit]("remote")
        .optional()
        .action{(_,c) => c.copy(remote = true)}
        .text("use remote decoder (default: false"),

      opt[Long]("blocks")
        .optional()
        .action{(x,c) => c.copy(blocks = x)}
        .text("blocks before yield (default: 1024"),

      opt[String]("remoteHost")
        .optional()
        .action{(x,c) => c.copy(remoteHost = x)}
        .text("remote host or ip address"),

      opt[Int]("remotePort")
        .optional()
        .action{(x,c) => c.copy(remotePort = x)}
        .text("remote port (default: 51770)"),

      opt[Int]("connections")
        .optional()
        .action{(x,c) => c.copy(nConnections = x)}
        .text("number of connections to remote receiver (default: 10"),

      opt[String]("pkgUri")
        .optional()
        .action{(x,c) => c.copy(pkgUri = x.stripSuffix("/"))}
        .text("GCS uri to tar file containing run.sh"),

      opt[String]("zone")
        .optional()
        .action{(x,c) => c.copy(zone = x)}
        .text("remote host zone"),

      opt[String]("subnet")
        .optional()
        .action{(x,c) => c.copy(subnet = x)}
        .text("remote host subnet in format " +
          "projects/<project>/regions/<region>/subnetworks/<subnet>"),

      opt[String]("serviceAccount")
        .optional()
        .action{(x,c) => c.copy(serviceAccount = x)}
        .text("remote host service account"),

      opt[String]("destPath")
        .optional
        .text("destination path")
        .action((x, c) => c.copy(destPath = x)),

      opt[String]("destDSN")
        .optional
        .text("destination DSN")
        .action((x, c) => c.copy(destDSN = x)),

      arg[String]("gcsUri")
        .required()
        .text("GCS URI in format (gs://bucket/path)")
        .validate{x =>
          val uri = new URI(x)
          if (uri.getScheme != "gs" || uri.getAuthority.isEmpty)
            failure("invalid GCS URI")
          else
            success
        }
        .action((x, c) => c.copy(gcsUri = x)),

      arg[String]("dest")
        .optional
        .text("(optional) local path or DSN (/path/to/file or DATASET.MEMBER or PDS(MBR))")
        .action{(x, c) =>
          if (x.contains("(")) c.copy(destDSN = x)
          else if (x.contains("/")) c.copy(destPath = x)
          else c.copy(destDSN = x)
        }
    )

  cmd("rm")
    .action((_,c) => c.copy(mode = "rm"))
    .text("Delete objects in GCS")
    .children(
      opt[Unit]('r',"recursive")
        .optional()
        .action{(_,c) => c.copy(recursive = true)}
        .text("delete directory"),

      opt[Unit]('f',"force")
        .optional()
        .text("delete without use interaction (always true)"),

      arg[String]("gcsUri")
        .required()
        .text("GCS URI in format (gs://bucket/path)")
        .validate{x =>
          val uri = new URI(x)
          if (uri.getScheme != "gs" || uri.getAuthority.isEmpty)
            failure("invalid GCS URI")
          else
            success
        }
        .action((x, c) => c.copy(gcsUri = x))
    )

  // Global Options from BigQuery
  opt[String]("dataset_id")
    .text(GlobalConfig.datasetIdText)
    .action((x,c) => c.copy(datasetId = x))

  opt[String]("location")
    .text(GlobalConfig.locationText)
    .action((x,c) => c.copy(location = x))

  opt[String]("project_id")
    .text(GlobalConfig.projectIdText)
    .action((x,c) => c.copy(projectId = x))

  // Custom Options
  opt[Unit]("allow_non_ascii")
    .text("allow non ascii characters")
    .action((_,c) => c.copy(allowNonAscii = true))

  opt[String]("stats_table")
    .optional()
    .text("tablespec of table to insert stats")
    .action((x,c) => c.copy(statsTable = x))

  opt[Double]("max_error_pct")
    .optional()
    .text("job failure threshold for row decoding errors (default: 0.0")
    .action((x,c) => c.copy(maxErrorPct = x))

  checkConfig{x =>
    if (x.remote) {
      if (x.remoteHost.isEmpty) {
        val missing = new StringBuilder
        if (x.subnet.isEmpty) missing.append(" --subnet")
        if (x.pkgUri.isEmpty) missing.append(" --pkgUri")
        if (x.serviceAccount.isEmpty) missing.append(" --serviceAccount")
        if (x.zone.isEmpty) missing.append(" --zone")

        if (x.projectId.isEmpty)
          failure("must specify --project_id if --remoteHost is set")
        else if (missing.nonEmpty)
          failure("must specify" + missing.result)
        else success
      } else success
    } else success
  }
}
