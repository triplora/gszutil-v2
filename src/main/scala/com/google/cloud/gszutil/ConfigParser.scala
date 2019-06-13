package com.google.cloud.gszutil

import com.google.cloud.pso.BQLoad
import scopt.OptionParser

import scala.util.{Success, Try}

object ConfigParser extends OptionParser[Config]("GSZUtil") {
  private val DefaultConfig = Config()
  def parse(args: Seq[String]): Option[Config] = parse(args, DefaultConfig)

  head("GSZUtil", "0.1.1")

  help("help").text("prints this usage text")

  cmd("load")
    .action { (_, c) => c.copy(mode = "load") }

    .text("loads a BigQuery Table")

    .children(
      arg[String]("bqProject")
        .required()
        .action { (x, c) => c.copy(bqProject = x) }
        .text("BigQuery Project ID"),

      arg[String]("bqDataset")
        .required()
        .action { (x, c) => c.copy(bqDataset = x) }
        .validate { x =>
          if (BQLoad.isValidBigQueryName(x)) success
          else failure(s"'$x' is not a valid dataset name")
        }
        .text("BigQuery Dataset name"),

      arg[String]("bqTable")
        .required()
        .action { (x, c) => c.copy(bqTable = x) }
        .validate { x =>
          if (BQLoad.isValidBigQueryName(x)) success
          else failure(s"'$x' is not a valid dataset name")
        }
        .text("BigQuery Table name"),

      arg[String]("bucket")
        .required()
        .action { (x, c) => c.copy(bqBucket = x) }
        .text("GCS bucket of source"),

      arg[String]("prefix")
        .required()
        .action { (x, c) => c.copy(bqPath = x) }
        .text("GCS prefix of source")
    )

  cmd("cp")
    .action { (_, c) => c.copy(mode = "cp") }

    .text("GSZUtil cp copies a zOS dataset to GCS")

    .children(
      arg[String]("dest")
        .required()
        .action { (x, c) =>
          Try(Util.parseUri(x)) match {
            case Success((bucket, path)) =>
              c.copy(dest = x, destBucket = bucket, destPath = path)
            case _ =>
              c.copy(dest = x)
          }
        }
        .text("destination path (gs://bucket/path)")
    )


  cmd("get")
    .action { (_, c) => c.copy(mode = "get") }

    .text("download a GCS object to UNIX filesystem")

    .children(
      arg[String]("source")
        .required()
        .action { (x, c) =>
          Try(Util.parseUri(x)) match {
            case Success((bucket, path)) =>
              c.copy(srcBucket = bucket, srcPath = path)
            case _ =>
              c
          }
        }
        .text("source path (/path/to/file)"),

      arg[String]("dest")
        .required()
        .action { (x, c) => c.copy(destPath = x)}
        .text("destination path (gs://bucket/path)")
    )

  opt[Boolean]("debug")
    .action { (x, c) => c.copy(debug = x) }
    .text("enable debug options (default: false)")

  opt[Int]("partSizeMB")
    .action{(x,c) => c.copy(partSizeMB = x)}
    .text("target part size in megabytes (default: 256)")

  opt[Int]("batchSize")
    .action{(x,c) => c.copy(batchSize = x)}
    .text("rows per batch (default: 10000)")

  opt[Int]('p', "parallelism")
    .action{(x,c) => c.copy(parallelism = x)}
    .text("number of concurrent writers (default: 5)")

  opt[Int]("timeOutMinutes")
    .action{(x,c) => c.copy(timeOutMinutes = x)}
    .text("timeout in minutes (default: 180)")

  checkConfig(c =>
    if (c.mode == "cp" && (c.destBucket.isEmpty || c.destPath.isEmpty))
      failure(s"invalid destination '${c.dest}'")
    else success
  )
}
