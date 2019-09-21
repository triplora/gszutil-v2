package com.google.cloud.gszutil

import java.net.URI

import com.google.cloud.bqsh.Bqsh
import com.google.cloud.gszutil.V2Server.V2Config
import scopt.OptionParser

object V2ConfigParser extends OptionParser[V2Config]("gReceiver") {
  def parse(args: Seq[String]): Option[V2Config] =
    parse(args, V2Config())

  head("gReceiver", Bqsh.UserAgent)

  help("help").text("prints this usage text")

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

  opt[Int]('n', "nWriters")
    .action{(x,c) => c.copy(nWriters = x)}
    .text("number of concurrent writers (default: 4)")

  arg[String]("bindAddress")
    .action{(x,c) => c.copy(host = x)}
    .text("Bind Address (default: 0.0.0.0)")

  opt[Int]('p', "port")
    .action{(x,c) => c.copy(nWriters = x)}
    .text("number of concurrent writers (default: 4)")

  opt[Int]("timeoutMinutes")
    .action{(x,c) => c.copy(timeoutMinutes = x)}
    .text("(optional) Timeout in minutes. (default: 1 day)")

  opt[Double]("max_error_pct")
    .text("job failure threshold for row decoding errors (default: 0.0")
    .action((x,c) => c.copy(maxErrorPct = x))
}
