package com.google.cloud.imf.util

import java.io.{PrintWriter, StringWriter}

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo

class StackDriverLogger(loggerName: String, log: Log) {
  val INFO = "INFO"
  val ERROR = "ERROR"
  val WARM = "WARN"
  val DEBUG = "DEBUG"

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

  def log(msg: String, zInfo: ZOSJobInfo, severity: String): Unit =
    log.logJson(stringData(msg, zInfo), severity)

  def logJson(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo, severity: String): Unit =
    log.logJson(jsonData(entries, zInfo), severity)

  def error(msg: String, throwable: Throwable, zInfo: ZOSJobInfo): Unit = {
    val m = stringData(msg, zInfo)
    if (throwable != null) {
      val w = new StringWriter()
      throwable.printStackTrace(new PrintWriter(w))
      m.put("throwable", throwable.getClass.getCanonicalName.stripSuffix("$"))
      m.put("stackTrace", w.toString)
    }
    log.logJson(m, ERROR)
  }
}
