package com.google.cloud.gszutil

import java.nio.file.Paths

import com.google.cloud.pso.BQLoad

import scala.util.{Success, Try}

object ConfigParser extends scopt.OptionParser[Config]("GSZUtil") {

  head("GSZUtil", "0.1.1")

  help("help").text("prints this usage text")

  cmd("load")
    .action { (_, c) => c.copy(mode = "load") }

    .text("loads a BigQuery Table")

    .children(
      arg[String]("bqProject")
        .required()
        .action { (x, c) => c.copy(bq = c.bq.copy(project = x)) }
        .text("BigQuery Project ID"),

      arg[String]("bqDataset")
        .required()
        .action { (x, c) => c.copy(bq = c.bq.copy(dataset = x)) }
        .validate { x =>
          if (BQLoad.isValidBigQueryName(x)) success
          else failure(s"'$x' is not a valid dataset name")
        }
        .text("BigQuery Dataset name"),

      arg[String]("bqTable")
        .required()
        .action { (x, c) => c.copy(bq = c.bq.copy(table = x)) }
        .validate { x =>
          if (BQLoad.isValidBigQueryName(x)) success
          else failure(s"'$x' is not a valid dataset name")
        }
        .text("BigQuery Table name"),

      arg[String]("bucket")
        .required()
        .action { (x, c) => c.copy(bq = c.bq.copy(bucket = x)) }
        .text("GCS bucket of source"),

      arg[String]("prefix")
        .required()
        .action { (x, c) => c.copy(bq = c.bq.copy(path = x)) }
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

  opt[Int]('p', "parallelism")
    .action{(x,c) => c.copy(parallelism = x)}
    .text("number of concurrent writers (default: 5)")

  checkConfig(c =>
    if (c.mode == "cp" && (c.destBucket.isEmpty || c.destPath.isEmpty))
      failure(s"invalid destination '${c.dest}'")
    else if (c.keyfile.nonEmpty && !Paths.get(c.keyfile).toFile.exists())
      failure(s"keyfile '${c.keyfile}' doesn't exist")
    else success
  )
}
