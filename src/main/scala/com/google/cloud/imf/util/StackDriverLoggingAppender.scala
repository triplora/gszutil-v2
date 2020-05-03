package com.google.cloud.imf.util

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level}

class StackDriverLoggingAppender extends AppenderSkeleton {

  private def toMap(e: LoggingEvent): java.util.Map[String,Any] = {
    val m = new java.util.HashMap[String,Any]
    m.put("logger",e.getLoggerName)
    m.put("thread",e.getThreadName)
    e.getMessage match {
      case s: String =>
        m.put("msg",s)
      case (k: String, v: String) =>
        m.put(k,v)
      case x: java.util.Map[_,_] =>
        x.forEach{
          case (k: String, v: Any) =>
            m.put(k,v)
          case _ =>
        }
      case x: Iterable[_] =>
        for (entry <- x) {
          entry match {
            case (k: String, v: Any) =>
              m.put(k,v)
            case _ =>
          }
        }
      case _ =>
        m.put("msg",e.getRenderedMessage)
    }
    m.put("timestamp", e.getTimeStamp)
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

  override def append(event: LoggingEvent): Unit =
    StackDriverLogging.logJson(toMap(event), sev(event.getLevel))

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = false
}
