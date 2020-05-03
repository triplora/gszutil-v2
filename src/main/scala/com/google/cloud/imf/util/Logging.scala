package com.google.cloud.imf.util

import java.io.{PrintWriter, StringWriter}

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import org.apache.log4j.{LogManager, Logger}

trait Logging {
  protected lazy val loggerName: String = this.getClass.getCanonicalName.stripSuffix("$")
  @transient
  protected lazy val logger: Logger = LogManager.getLogger(loggerName)

  private def stringData(msg: String, zInfo: ZOSJobInfo): java.util.Map[String,Any] = {
    val m = new java.util.HashMap[String,Any]()
    m.put("msg", msg)
    m.put("logger", loggerName)
    Util.putInfo(zInfo, m)
    m
  }

  private def jsonData(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): java.util.Map[String,Any] = {
    val m = new java.util.HashMap[String,Any]()
    m.put("logger", loggerName)
    for ((k,v) <- entries){
      m.put(k,v)
    }
    Util.putInfo(zInfo, m)
    m
  }

  def info(msg: String, zInfo: ZOSJobInfo): Unit = {
    StackDriverLogging.logJson(stringData(msg, zInfo), "INFO")
    logger.info(msg)
  }

  def info(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    val m = jsonData(entries, zInfo)
    StackDriverLogging.logJson(m, "INFO")
    val msg = m.get("msg")
    if (msg != null)
      logger.info(msg)
  }

  def error(entries: Iterable[(String,String)], zInfo: ZOSJobInfo): Unit = {
    error(entries, null, zInfo)
  }

  def error(entries: Iterable[(String,String)], t: Throwable, zInfo: ZOSJobInfo): Unit = {
    val data = jsonData(entries, zInfo)
    StackDriverLogging.logJson(data, "ERROR")
    val msg = data.get("msg")
    if (msg != null) {
      if (t != null) logger.error(msg, t)
      else logger.error(msg)
    }
  }

  def error(msg: String, zInfo: ZOSJobInfo): Unit = error(msg, null, zInfo)

  def error(msg: String, t: Throwable, zInfo: ZOSJobInfo): Unit = {
    val m = stringData(msg, zInfo)
    if (t != null) {
      val w = new StringWriter()
      t.printStackTrace(new PrintWriter(w))
      m.put("throwable", t.getClass.getCanonicalName.stripSuffix("$"))
      m.put("stackTrace", w.toString)
    }
    StackDriverLogging.logJson(m, "ERROR")
    if (t != null) logger.error(m, t)
    else logger.error(msg)
  }

  def warn(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = stringData(msg, zInfo)
    StackDriverLogging.logJson(m, "WARN")
    logger.warn(m)
  }

  def warn(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    val m = jsonData(entries, zInfo)
    StackDriverLogging.logJson(m, "WARN")
    val msg = m.get("msg")
    if (msg != null)
      logger.warn(msg)
  }

  def debug(msg: String, zInfo: ZOSJobInfo): Unit = {
    if (logger.isDebugEnabled){
      val m = stringData(msg, zInfo)
      StackDriverLogging.logJson(m, "DEBUG")
      logger.debug(m)
    }
  }

  def debug(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    if (logger.isDebugEnabled){
      val m = jsonData(entries, zInfo)
      StackDriverLogging.logJson(m, "DEBUG")
      val msg = m.get("msg")
      if (msg != null)
        logger.debug(msg)
    }
  }
}
