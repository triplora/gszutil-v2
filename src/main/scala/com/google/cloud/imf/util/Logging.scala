package com.google.cloud.imf.util

import java.io.{PrintWriter, StringWriter}

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import org.apache.log4j.{LogManager, Logger}

trait Logging {
  protected lazy val loggerName: String = this.getClass.getCanonicalName.stripSuffix("$")
  @transient
  protected lazy val logger: Logger = LogManager.getLogger(loggerName)

  @transient
  protected lazy val sdLogger: StackDriverLogger = new StackDriverLogger(loggerName)
}
