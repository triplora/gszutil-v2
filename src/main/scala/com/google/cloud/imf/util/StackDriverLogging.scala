package com.google.cloud.imf.util

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.logging.v2.Logging
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.gszutil.CCATransportFactory

object StackDriverLogging {
  private var instance: Log = NoOpLog

  def init(credentials: Credentials, project: String, logId: String): Unit = {
    instance = new StackDriverLog(new Logging.Builder(CCATransportFactory.Instance,
      JacksonFactory.getDefaultInstance,
      new HttpCredentialsAdapter(credentials))
      .setApplicationName("mainframe-connector").build, project, logId)
  }

  def getLogger(loggerName: String): StackDriverLogger =
    new StackDriverLogger(loggerName, instance)

  def getLogger(cls: Class[_]): StackDriverLogger =
    new StackDriverLogger(cls.getSimpleName.stripSuffix("$"), instance)
}
