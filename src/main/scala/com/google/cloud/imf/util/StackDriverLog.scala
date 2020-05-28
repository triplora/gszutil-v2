package com.google.cloud.imf.util

import com.google.api.services.logging.v2.Logging
import com.google.api.services.logging.v2.model.{LogEntry, MonitoredResource, WriteLogEntriesRequest}
import com.google.common.collect.ImmutableList

class StackDriverLog(client: Logging, project: String, logId: String) extends Log {
  private val logName: String = s"projects/$project/logs/$logId"
  private val resource: MonitoredResource = new MonitoredResource().setType("global")
  System.out.println(s"Initialized StackDriver Logging for '$logName'")

  override def log(msg: String, severity: String): Unit = {
    if (client != null) {
      val entry: LogEntry = new LogEntry()
        .setTextPayload(msg)
        .setSeverity(severity)
        .setLogName(logName)
        .setResource(resource)
      val req = new WriteLogEntriesRequest()
        .setLogName(logName)
        .setResource(resource)
        .setEntries(ImmutableList.of(entry))
      client.entries.write(req).execute
    }
  }

  override def logJson(data: java.util.Map[String, Any], severity: String): Unit = {
    if (client != null && data != null && severity != null) {
      val entry: LogEntry = new LogEntry()
        .setJsonPayload(data.asInstanceOf[java.util.Map[String,Object]])
        .setSeverity(severity)
        .setLogName(logName)
        .setResource(resource)
      val req: WriteLogEntriesRequest = new WriteLogEntriesRequest()
        .setLogName(logName)
        .setResource(resource)
        .setEntries(ImmutableList.of(entry))
      client.entries.write(req).execute
    }
  }
}
