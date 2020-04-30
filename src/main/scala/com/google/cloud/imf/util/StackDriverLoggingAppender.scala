package com.google.cloud.imf.util

import java.util

import com.google.api.services.logging.v2.Logging
import com.google.api.services.logging.v2.model.{LogEntry, MonitoredResource, WriteLogEntriesRequest}
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.common.collect.ImmutableList
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level}

/**
  *
  * @param logName projects/[PROJECT_ID]/logs/[LOG_ID]
  * @param stackDriver Logging
  */
case class StackDriverLoggingAppender(logName: String, stackDriver: Logging)
  extends AppenderSkeleton {
  private val resource: MonitoredResource = new MonitoredResource().setType("global")

  def log(msg: String, severity: String): Unit = {
    val entry: LogEntry = new LogEntry()
      .setTextPayload(msg)
      .setSeverity(severity)
      .setLogName(logName)
      .setResource(resource)
    val req = new WriteLogEntriesRequest()
      .setLogName(logName)
      .setResource(resource)
      .setEntries(ImmutableList.of(entry))
    stackDriver.entries.write(req).execute
  }

  def log(data: java.util.Map[String, Object], severity: String): Unit = {
    if (data != null && severity != null) {
      val entry: LogEntry = new LogEntry()
        .setJsonPayload(data)
        .setSeverity(severity)
        .setLogName(logName)
        .setResource(resource)
      val req = new WriteLogEntriesRequest()
        .setLogName(logName)
        .setResource(resource)
        .setEntries(ImmutableList.of(entry))
      stackDriver.entries.write(req).execute
    }
  }

  private def toMap(e: LoggingEvent): java.util.Map[String,Object] = {
    val m = new util.HashMap[String,Object]
    m.put("name",e.getLoggerName)
    m.put("thread",e.getThreadName)
    e.getMessage match {
      case s: String =>
        m.put("msg",s)
      case (k: String, v: String) =>
        m.put(k,v)
      case x: java.util.Map[String,String] =>
        x.forEach{(k,v) => m.put(k,v)}
      case x: Iterable[(String,String)] =>
        for ((k,v) <- x) m.put(k,v)
      case _ =>
        m.put("msg",e.getRenderedMessage)
    }
    m.put("timestamp", e.getTimeStamp.asInstanceOf[Object])
    if (e.getThrowableInformation != null){
      val w = new ByteArrayWriter
      e.getThrowableInformation
        .getThrowable.printStackTrace()
      m.put("stackTrace", w.result)
    }
    m
  }

  private def sev(l: Level): String = {
    import Level._
    l match {
      case INFO => "INFO"
      case DEBUG => "DEBUG"
      case FATAL => "CRITICAL"
      case TRACE => "DEBUG"
      case WARN => "WARNING"
      case OFF => null
      case _ => "DEFAULT"
    }
  }

  override def append(event: LoggingEvent): Unit = log(toMap(event), sev(event.getLevel))

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = false
}
