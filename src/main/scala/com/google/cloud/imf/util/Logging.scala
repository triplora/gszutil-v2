package com.google.cloud.imf.util

import java.util

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import org.apache.log4j.{LogManager, Logger}

trait Logging {
  @transient
  protected lazy val logger: Logger = LogManager.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))

  def info(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = new util.HashMap[String,Object]()
    m.put("msg", msg)
    Util.putInfo(zInfo, m)
    logger.info(m)
  }

  def error(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = new util.HashMap[String,Object]()
    m.put("msg", msg)
    Util.putInfo(zInfo, m)
    logger.error(m)
  }

  def error(msg: String, t: Throwable, zInfo: ZOSJobInfo): Unit = {
    val m = new util.HashMap[String,Object]()
    m.put("msg", msg)
    Util.putInfo(zInfo, m)
    logger.error(m, t)
  }

  def warn(msg: String, zInfo: ZOSJobInfo): Unit = {
    val m = new util.HashMap[String,Object]()
    m.put("msg", msg)
    Util.putInfo(zInfo, m)
    logger.warn(m)
  }

  def debug(msg: String, zInfo: ZOSJobInfo): Unit = {
    if (logger.isDebugEnabled){
      val m = new util.HashMap[String,Object]()
      m.put("msg", msg)
      Util.putInfo(zInfo, m)
      logger.debug(m)
    }
  }
}
