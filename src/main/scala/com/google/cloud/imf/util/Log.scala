package com.google.cloud.imf.util

trait Log {
  def log(msg: String, severity: String): Unit
  def logJson(data: java.util.Map[String, Any], severity: String): Unit
}
