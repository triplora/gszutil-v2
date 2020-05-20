package com.google.cloud.imf.util

import java.io.{PrintWriter, StringWriter}

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo

class StackDriverLogger(loggerName: String) {
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
  }

  def info(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    val m = jsonData(entries, zInfo)
    StackDriverLogging.logJson(m, "INFO")
  }

  def error(entries: Iterable[(String,String)], zInfo: ZOSJobInfo): Unit = {
    error(entries, null, zInfo)
  }

  def error(entries: Iterable[(String,String)], t: Throwable, zInfo: ZOSJobInfo): Unit = {
    val data = jsonData(entries, zInfo)
    StackDriverLogging.logJson(data, "ERROR")
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
  }

  def warn(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = stringData(msg, zInfo)
    StackDriverLogging.logJson(m, "WARN")
  }

  def warn(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    val m = jsonData(entries, zInfo)
    StackDriverLogging.logJson(m, "WARN")
  }

  def debug(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = stringData(msg, zInfo)
    StackDriverLogging.logJson(m, "DEBUG")
  }

  def debug(entries: Iterable[(String,Any)], zInfo: ZOSJobInfo): Unit = {
    val m = jsonData(entries, zInfo)
    StackDriverLogging.logJson(m, "DEBUG")
  }
}
