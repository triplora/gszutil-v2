package com.google.cloud.imf.util

import java.util

import com.google.cloud.MonitoredResource
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.logging.{LogEntry, Logging, Payload, Severity}
import com.google.common.collect.ImmutableList
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level}

case class StackDriverLoggingAppender(log: String,
                                      stackDriver: Logging) extends AppenderSkeleton {
  private val resource: MonitoredResource = MonitoredResource.newBuilder("global").build
  private var jobInfo: ZOSJobInfo = _
  def setJobInfo(zInfo: ZOSJobInfo): Unit = if (jobInfo == null) jobInfo = zInfo

  def log(msg: String, severity: Severity): Unit = {
    if (severity != null){
      val entry = LogEntry.newBuilder(Payload.StringPayload.of(msg))
        .setSeverity(severity)
        .setLogName(log)
        .setResource(resource).build
      stackDriver.write(ImmutableList.of(entry))
    }
  }

  def log(data: java.util.Map[String, Any], severity: Severity): Unit = {
    if (data != null && severity != null) {
      stackDriver.write(ImmutableList.of(
        LogEntry.newBuilder(Payload.JsonPayload.of(data))
          .setSeverity(severity)
          .setLogName(log)
          .setResource(resource)
          .build
      ))
    }
  }

  private def toMap(e: LoggingEvent): java.util.Map[String,Any] = {
    val m = new util.HashMap[String,Any]()
    m.put("name",e.getLoggerName)
    m.put("thread",e.getThreadName)
    m.put("msg",e.getRenderedMessage)
    m.put("timestamp", e.getTimeStamp)
    if (e.getThrowableInformation != null){
      val w = new ByteArrayWriter()
      e.getThrowableInformation
        .getThrowable.printStackTrace()
      m.put("stackTrace", w.result)
    }
    if (jobInfo != null){
      m.put("jobname", jobInfo.getJobname)
      m.put("jobdate", jobInfo.getJobdate)
      m.put("jobtime", jobInfo.getJobtime)
      m.put("procstepname",jobInfo.getProcStepName)
      m.put("stepname",jobInfo.getStepName)
      m.put("user",jobInfo.getUser)
      m.put("jobid",jobInfo.getJobid)
    }
    m
  }

  private def sev(l: Level): Severity = {
    import Level._
    l match {
      case INFO => Severity.INFO
      case DEBUG => Severity.DEBUG
      case FATAL => Severity.CRITICAL
      case TRACE => Severity.DEBUG
      case WARN => Severity.WARNING
      case OFF => null
      case _ => Severity.DEFAULT
    }
  }

  override def append(event: LoggingEvent): Unit = log(toMap(event), sev(event.getLevel))

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = false
}
