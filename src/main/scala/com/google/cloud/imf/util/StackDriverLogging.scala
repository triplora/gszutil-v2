package com.google.cloud.imf.util

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.logging.v2.Logging
import com.google.api.services.logging.v2.model.{LogEntry, MonitoredResource, WriteLogEntriesRequest}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.gszutil.CCATransportFactory
import com.google.common.collect.ImmutableList

object StackDriverLogging {
  private var Instance: com.google.api.services.logging.v2.Logging = _
  private var LogName: String = "projects/[PROJECT_ID]/logs/[LOG_ID]"
  private val Resource: MonitoredResource = new MonitoredResource().setType("global")

  def getInstance: com.google.api.services.logging.v2.Logging = Instance

  def init(credentials: Credentials, logName: String): Unit = {
    System.out.println(s"Initializing StackDriver Logging for log: '$logName'")
    Instance = new Logging.Builder(CCATransportFactory.Instance,
      JacksonFactory.getDefaultInstance,
      new HttpCredentialsAdapter(credentials))
      .setApplicationName("mainframe-connector").build
    LogName = logName
  }

  def log(msg: String, severity: String): Unit = {
    if (Instance != null) {
      val entry: LogEntry = new LogEntry()
        .setTextPayload(msg)
        .setSeverity(severity)
        .setLogName(LogName)
        .setResource(Resource)
      val req = new WriteLogEntriesRequest()
        .setLogName(LogName)
        .setResource(Resource)
        .setEntries(ImmutableList.of(entry))
      Instance.entries.write(req).execute
    }
  }

  def logJson(data: java.util.Map[String, Any], severity: String): Unit = {
    if (Instance != null && data != null && severity != null) {
      val entry: LogEntry = new LogEntry()
        .setJsonPayload(data.asInstanceOf[java.util.Map[String,Object]])
        .setSeverity(severity)
        .setLogName(LogName)
        .setResource(Resource)
      val req: WriteLogEntriesRequest = new WriteLogEntriesRequest()
        .setLogName(LogName)
        .setResource(Resource)
        .setEntries(ImmutableList.of(entry))
      Instance.entries.write(req).execute
    }
  }
}
