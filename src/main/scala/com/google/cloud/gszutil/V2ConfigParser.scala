package com.google.cloud.gszutil

import com.google.cloud.bqsh.Bqsh
import com.google.cloud.gszutil.V2Server.V2Config
import scopt.OptionParser

object V2ConfigParser extends OptionParser[V2Config]("gReceiver") {
  def parse(args: Seq[String]): Option[V2Config] =
    parse(args, V2Config())

  head("gReceiver", Bqsh.UserAgent)

  help("help").text("prints this usage text")

  opt[Int]('n', "nWriters")
    .optional()
    .action{(x,c) => c.copy(nWriters = x)}
    .text("number of concurrent writers (default: 4)")

  opt[Int]("bufCt")
    .optional()
    .action{(x,c) => c.copy(bufCt = x)}
    .text("number of buffers (default: 32)")

  opt[String]("bindAddress")
    .optional()
    .action{(x,c) => c.copy(host = x)}
    .text("Bind Address (default: 127.0.0.1)")

  opt[Int]('p', "port")
    .optional()
    .action{(x,c) => c.copy(nWriters = x)}
    .text("number of concurrent writers (default: 4)")

  opt[Int]("timeoutMinutes")
    .optional()
    .action{(x,c) => c.copy(timeoutMinutes = x)}
    .text("(optional) Timeout in minutes. (default: 1 day)")

  opt[Unit]("daemon")
    .optional()
    .action{(_,c) => c.copy(daemon = true)}
    .text("(optional) Wait for another session after completion (default: false)")

  opt[Double]("max_error_pct")
    .optional()
    .text("job failure threshold for row decoding errors (default: 0.0")
    .action((x,c) => c.copy(maxErrorPct = x))
}
