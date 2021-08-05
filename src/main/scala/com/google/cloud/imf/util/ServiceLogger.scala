package com.google.cloud.imf.util

import com.google.cloud.imf.gzos.pb.GRecvProto
import org.apache.log4j.Logger

trait ServiceLogger {
  def info(message: String)(implicit logger: Logger): Unit =
    logger.info(message)

  def error(message: String, t: Throwable)(implicit logger: Logger): Unit =
    logger.error(message, t)

  def error(message: String)(implicit logger: Logger): Unit =
    logger.error(message)
}

object DefaultLog extends ServiceLogger

case class GrecvLog(jobInfo: GRecvProto.ZOSJobInfo) extends ServiceLogger {
  private val jobId = s"Job[${jobInfo.getJobname}:${jobInfo.getJobid}:${jobInfo.getStepName}]"
  private val messageF = (x: String) => s"$jobId. $x"

  override def info(message: String)(implicit logger: Logger): Unit =
    super.info(messageF(message))

  override def error(message: String, t: Throwable)(implicit logger: Logger): Unit =
    super.error(messageF(message), t)

  override def error(message: String)(implicit logger: Logger): Unit =
    super.error(messageF(message))
}
