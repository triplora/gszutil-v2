package com.google.cloud.imf.util

object NoOpLog extends Log {
  override def log(msg: String, severity: String): Unit = {}
  override def logJson(data: java.util.Map[String, Any], severity: String): Unit = {}
}
